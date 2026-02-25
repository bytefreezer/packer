// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/api"
	"github.com/bytefreezer/packer/config"
	"github.com/bytefreezer/packer/services"
	"github.com/bytefreezer/packer/storage"
	"github.com/pkg/errors"
)

var (
	version   = "dev"
	buildTime = "unknown"
	gitCommit = "unknown"
	conf      = config.Config{}
	envPrefix = "BYTEFREEZER_"
)

// getDockerContainerID returns the short container ID if running inside Docker, empty string otherwise.
func getDockerContainerID() string {
	if _, err := os.Stat("/.dockerenv"); err != nil {
		return ""
	}
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		if idx := strings.LastIndex(line, "/docker/"); idx != -1 {
			id := line[idx+len("/docker/"):]
			if len(id) >= 12 {
				return id[:12]
			}
		}
		if idx := strings.LastIndex(line, "/docker-"); idx != -1 {
			id := strings.TrimSuffix(line[idx+len("/docker-"):], ".scope")
			if len(id) >= 12 {
				return id[:12]
			}
		}
	}
	data, err = os.ReadFile("/proc/1/cpuset")
	if err != nil {
		return ""
	}
	cpuset := strings.TrimSpace(string(data))
	id := filepath.Base(cpuset)
	if len(id) >= 12 && id != "/" {
		return id[:12]
	}
	return ""
}

// Execute executes the root command.
func Run() error {
	var (
		cfgFilePath = flag.String("config", "config.yaml", "Path to configuration file")
		showVersion = flag.Bool("version", false, "Show version and exit")
		showHelp    = flag.Bool("help", false, "Show help and exit")
		clearLocks  = flag.Bool("clear-locks", false, "Clear all locks via Control API and exit")
	)

	flag.Parse()

	// Handle version flag
	if *showVersion {
		fmt.Printf("bytefreezer-packer version %s (built %s, commit %s)\n", version, buildTime, gitCommit)
		os.Exit(0)
	}

	// Handle help flag
	if *showHelp {
		fmt.Printf("ByteFreezer Packer - Data processing and packaging service\n\n")
		fmt.Printf("Usage: %s [options]\n\n", os.Args[0])
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		os.Exit(0)
	}

	// Suppress AWS SDK warnings about checksums (MinIO compatibility)
	stdlog.SetOutput(io.Discard)

	// Silence AWS SDK logger warnings (checksum warnings, etc)
	os.Setenv("AWS_SDK_LOG_LEVEL", "error")

	fmt.Printf("Loading configuration from: %s\n", *cfgFilePath)
	err := config.LoadConfig(*cfgFilePath, envPrefix, &conf)
	if err != nil {
		return errors.Wrap(err, "failed to load config")
	}

	// Set runtime build info from ldflags (overrides config file values)
	conf.App.Version = version

	setLogLevel(conf.Logging.Level)

	// Handle clear-locks flag
	if *clearLocks {
		return clearAllLocks(*cfgFilePath)
	}

	// Initialize OpenTelemetry provider if enabled
	var otelShutdown func()
	if conf.Otel.Enabled {
		otelShutdown = InitOtelProvider(&conf)
		defer func() {
			if otelShutdown != nil {
				log.Info("Shutting down OpenTelemetry provider")
				otelShutdown()
			}
		}()
		log.Info("OpenTelemetry provider initialized")
	}

	// Initialize all components - fail hard if this fails
	log.Info("Initializing service components...")
	if err := conf.InitializeComponents(); err != nil {
		return errors.Wrap(err, "failed to initialize service components - service cannot start")
	}

	// Log successful initialization
	bucketName, endpoint, ssl := conf.S3SourceClient.GetBucketInfo()
	log.Infof("S3 source client initialized successfully - bucket: %s, endpoint: %s, SSL: %t",
		bucketName, endpoint, ssl)
	log.Infof("Tenant database initialized successfully with %d tenants", conf.TenantDatabase.GetTenantCount())

	// Send service start alert
	if err := conf.SOCAlertClient.SendServiceStartAlert(); err != nil {
		log.Warnf("Failed to send service start alert: %v", err)
	}

	// Cleanup locks from this instance (abandoned from previous runs/crashes)
	hostname := getDockerContainerID()
	if hostname == "" {
		hostname, _ = os.Hostname()
	}
	if hostname == "" {
		hostname = "packer-unknown"
	}
	log.Infof("Cleaning up abandoned locks from previous runs of this instance (%s)...", hostname)
	if heartbeatClient, ok := conf.GetLockClient().(storage.HeartbeatLockClient); ok {
		// First, cleanup any locks held by this specific instance (from crashes/restarts)
		if err := heartbeatClient.CleanupInstanceLocks(context.Background(), hostname); err != nil {
			log.Warnf("Failed to cleanup instance locks on startup: %v", err)
		} else {
			log.Infof("Instance lock cleanup completed successfully for %s", hostname)
		}

		// Then cleanup any other stale locks (from other instances that may have crashed)
		// Only cleanup locks that haven't been updated in 10 minutes
		if err := heartbeatClient.CleanupStaleLocks(context.Background(), 10); err != nil {
			log.Warnf("Failed to cleanup stale locks on startup: %v", err)
		} else {
			log.Info("Stale lock cleanup completed successfully")
		}
	} else {
		log.Debug("No heartbeat-capable lock client available for startup cleanup")
	}

	// Cleanup stale operations from previous runs (self-healing)
	log.Infof("Cleaning up stale operations from previous runs...")
	cleanupStaleOperations(&conf)

	// Business Logic
	servicesInstance := services.NewServices(&conf)

	//start stab server
	server := NewServer(servicesInstance, &conf)

	server.HttpApi = api.NewAPI(servicesInstance, &conf)

	// Initialize processing orchestrator with spool-based reliability
	orchestrator, err := services.NewProcessingOrchestrator(&conf)
	if err != nil {
		log.Fatalf("Failed to create processing orchestrator: %v", err)
	}

	if err := orchestrator.Start(); err != nil {
		log.Fatalf("Failed to start processing orchestrator: %v", err)
	}
	defer orchestrator.Stop()

	// Set the orchestrator in the API for processing endpoints
	server.HttpApi.SetProcessingOrchestrator(orchestrator)

	// Initialize health reporting service
	var healthReportingService *services.HealthReportingService
	if conf.HealthReporting.Enabled && conf.ControlService.ControlURL != "" {
		reportInterval := time.Duration(conf.HealthReporting.ReportInterval) * time.Second
		if reportInterval <= 0 {
			reportInterval = 5 * time.Minute // Default 5 minutes
			log.Warnf("Invalid report_interval %d, using default %v", conf.HealthReporting.ReportInterval, reportInterval)
		}

		timeout := time.Duration(conf.HealthReporting.TimeoutSeconds) * time.Second
		if timeout <= 0 {
			timeout = 30 * time.Second // Default 30 seconds
		}

		// Get instance ID: prefer Docker container ID, then hostname
		// If running in Kubernetes with NODE_NAME env var, use node.pod format
		hostname := getDockerContainerID()
		if hostname != "" {
			log.Infof("Running in Docker container, instance ID: %s", hostname)
		} else {
			var err error
			hostname, err = os.Hostname()
			if err != nil {
				log.Warnf("Failed to get hostname, using 'localhost': %v", err)
				hostname = "localhost"
			}
		}
		if nodeName := os.Getenv("NODE_NAME"); nodeName != "" && nodeName != hostname {
			// In K8s: hostname is the pod name, NODE_NAME is the actual node
			hostname = fmt.Sprintf("%s.%s", nodeName, hostname)
			log.Infof("Running in Kubernetes on node %s, instance ID: %s", nodeName, hostname)
		}

		// Determine instance API URL without protocol (packer API endpoint)
		instanceAPI := fmt.Sprintf("%s:%d", hostname, conf.Server.ApiPort)

		// Build configuration data with masked sensitive fields
		configuration := buildHealthConfiguration(&conf, instanceAPI)

		healthReportingService = services.NewHealthReportingService(
			conf.ControlService.ControlURL,
			"bytefreezer-packer",
			instanceAPI,
			conf.ControlService.APIKey, // Pass API key for Bearer token authentication
			reportInterval,
			timeout,
			configuration,
		)

		// Register service on startup if enabled
		if conf.HealthReporting.RegisterOnStartup {
			if err := healthReportingService.RegisterService(); err != nil {
				log.Warnf("Failed to register service on startup: %v", err)
			}
		}

		// Start health reporting
		healthReportingService.Start()
		defer healthReportingService.Stop()
		log.Infof("Health reporting service started - reporting to %s every %v", conf.ControlService.ControlURL, reportInterval)

		// Listen for uninstall directive from control plane
		go func() {
			<-healthReportingService.UninstallChan()
			log.Warnf("Received uninstall directive from control plane — shutting down and self-removing")
			healthReportingService.Stop()
			selfCleanup("bytefreezer-packer")
			os.Exit(0)
		}()
	} else {
		log.Info("Health reporting is disabled")
	}

	// Track tenant update failures for SOC alerting
	var tenantUpdateFailures int

	// Define housekeeping function for tenant database updates and processing
	housekeepingFn := func() {
		log.Debug("Starting housekeeping cycle...")

		// Step 1: Update tenant database
		log.Debug("Updating tenant database during housekeeping...")
		if err := conf.UpdateTenantDatabase(); err != nil {
			tenantUpdateFailures++
			log.Errorf("Failed to update tenant database during housekeeping: %v", err)

			// Send SOC alert for repeated failures
			if tenantUpdateFailures >= 3 && (tenantUpdateFailures%3 == 0) { // Alert every 3rd failure after the first 3
				if alertErr := conf.SOCAlertClient.SendTenantUpdateFailureAlert(err, tenantUpdateFailures); alertErr != nil {
					log.Warnf("Failed to send tenant update failure alert: %v", alertErr)
				}
			}
			return // Don't process tenants if database update failed
		} else {
			// Reset failure count on success
			if tenantUpdateFailures > 0 {
				log.Infof("Tenant database update recovered after %d failures", tenantUpdateFailures)
				tenantUpdateFailures = 0
			}
			log.Debugf("Tenant database updated successfully, current count: %d", conf.TenantDatabase.GetTenantCount())
		}

		// Step 2: Schedule tenant datasets for processing (orchestrator handles the actual processing)
		log.Debug("Scheduling tenant datasets for processing...")
		tenants := conf.TenantDatabase.GetAllTenants()
		for _, tenant := range tenants {
			for _, dataset := range tenant.Datasets {
				// Schedule each dataset for processing with normal priority
				if err := orchestrator.ScheduleDatasetProcessing(tenant.ID, dataset.ID, 1); err != nil {
					log.Errorf("Failed to schedule dataset %s/%s for processing: %v", tenant.ID, dataset.ID, err)
				}
			}
		}

		// Log summary of orchestrator stats
		stats, err := orchestrator.GetProcessingStats()
		if err != nil {
			log.Warnf("Failed to get processing stats: %v", err)
		} else {
			log.Infof("Housekeeping completed: orchestrator stats: %+v", stats)
		}

		// Step 3: Cleanup cache directories (handled by orchestrator)
		if conf.Housekeeping.Cleanup.Enabled {
			log.Debug("Cleaning up cache directories...")
			// Cache cleanup is now handled by the orchestrator's housekeeping
			log.Debug("Cache cleanup handled by orchestrator housekeeping")
		}

		// Step 4: Cleanup stale locks (if lock client is available and enabled)
		if conf.Housekeeping.Cleanup.LockCleanup.Enabled {
			if heartbeatClient, ok := conf.GetLockClient().(storage.HeartbeatLockClient); ok {
				log.Debug("Cleaning up stale locks...")
				staleThresholdMinutes := conf.Housekeeping.Cleanup.LockCleanup.StaleThresholdMinutes
				if staleThresholdMinutes <= 0 {
					staleThresholdMinutes = 5 // Default to 5 minutes
				}
				if err := heartbeatClient.CleanupStaleLocks(context.Background(), staleThresholdMinutes); err != nil {
					log.Warnf("Failed to cleanup stale locks: %v", err)
				}
			} else {
				log.Debug("Lock cleanup enabled but no heartbeat-capable lock client available")
			}
		}

		log.Debug("Housekeeping cycle completed")
	}

	//start server
	go server.Start(housekeepingFn, nil)

	//start api server
	server.HttpApi.Serve(":"+strconv.Itoa(conf.Server.ApiPort), server.HttpApi.NewRouter())

	return nil
}

func setLogLevel(levelStr string) {
	switch strings.ToLower(levelStr) {
	case "debug":
		log.SetMinLogLevel(log.MinLevelDebug)
	case "info":
		log.SetMinLogLevel(log.MinLevelInfo)
	case "warn":
		log.SetMinLogLevel(log.MinLevelWarn)
	case "error":
		log.SetMinLogLevel(log.MinLevelError)
	}
}

// Server provides basic service functions and state common to all service types
type Server struct {
	Config   *config.Config
	Name     string
	quitterC chan time.Duration // also internal-only
	HttpApi  *api.API
	Services *services.Services
	//here you can add other services
}

func NewServer(services *services.Services, conf *config.Config) *Server {
	return &Server{
		Config:   conf,
		Name:     conf.App.Name,
		quitterC: make(chan time.Duration),
		Services: services,
	}
}

func (svc *Server) Start(housekeepingFn func(), quitterFn func(time.Duration)) {

	// exit cleanly on signal
	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, syscall.SIGINT, syscall.SIGQUIT, syscall.SIGABRT, syscall.SIGTERM)
	go func() {
		sig := <-signalC
		log.Debugf("Received signal %v", sig)

		if err := svc.Stop(2 * time.Second); err != nil {
			log.Fatalf("error stopping service: %v", err)
		}
	}()

	baseInterval := time.Duration(svc.Config.Housekeeping.IntervalSeconds) * time.Second

	if baseInterval <= 0 {
		baseInterval = 10 * time.Second
		log.Errorf("invalid housekeeping-interval: %d", baseInterval)
	}

	log.Infof("Starting housekeeping with base interval %v (randomized 1x-2x for load balancing)", baseInterval)

	// Run initial housekeeping immediately on startup
	log.Info("Running initial housekeeping on startup...")
	if housekeepingFn != nil && svc.Config.Housekeeping.Enabled {
		housekeepingFn()
	}

	// Continue with randomized intervals for subsequent cycles
	for {
		// Generate random interval between baseInterval and 2*baseInterval
		randomMultiplier := 1.0 + rand.Float64() // #nosec G404 - non-cryptographic random for timing jitter
		nextInterval := time.Duration(float64(baseInterval) * randomMultiplier)

		log.Debugf("Next housekeeping in %v (base: %v, multiplier: %.2f)", nextInterval, baseInterval, randomMultiplier)

		timer := time.NewTimer(nextInterval)

		select {
		case <-timer.C:
			log.Debug("housekeeping")

			if housekeepingFn != nil && svc.Config.Housekeeping.Enabled {
				housekeepingFn()
			}

		case timeout := <-svc.quitterC:
			log.Debug("exiting service")
			timer.Stop()

			// Cleanup operations immediately - don't wait for completion
			log.Info("Cleaning up in-progress operations before shutdown...")
			cleanupStaleOperations(svc.Config)

			if quitterFn != nil {
				quitterFn(timeout)
			}

			// Health reporting service will be stopped via defer in main function

			svc.HttpApi.Stop()

			return
		}
	}
}

func (svc *Server) Stop(timeout time.Duration) error {
	log.Debugf("sending timeout %s to quitterC:", timeout)

	// Try to send timeout to main loop, wait up to the timeout duration
	select {
	case svc.quitterC <- timeout:
		log.Debug("sent timeout to quitterC - graceful shutdown initiated")
	case <-time.After(timeout):
		log.Warn("timed out sending shutdown signal - forcing shutdown")
		// Main loop didn't respond within timeout, force close
		select {
		case <-svc.quitterC:
			// Channel already closed, nothing to do
			log.Debug("quitterC already closed")
		default:
			// Force close the channel to unblock main loop
			close(svc.quitterC)
			log.Debug("forced close of quitterC channel")
		}
	}
	return nil
}

// clearAllLocks clears all locks via Control API for clean restarts
func clearAllLocks(configPath string) error {
	var cfg config.Config
	err := config.LoadConfig(configPath, envPrefix, &cfg)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Check if Control Service is configured
	if cfg.ControlService.ControlURL == "" {
		fmt.Printf("Control Service not configured - missing 'control_service:' section in config file\n")
		fmt.Printf("Cannot clear locks without Control Service configuration\n")
		return nil
	}

	fmt.Printf("Connecting to Control Service at %s...\n", cfg.ControlService.ControlURL)

	// Initialize components to set up lock client
	err = cfg.InitializeComponents()
	if err != nil {
		return fmt.Errorf("failed to initialize lock client: %w", err)
	}

	// Get the lock client
	lockClient := cfg.GetLockClient()
	if lockClient == nil {
		return fmt.Errorf("lock client not initialized")
	}

	// Clear all locks
	fmt.Printf("Clearing all locks from lock service...\n")
	ctx := context.Background()

	if apiClient, ok := lockClient.(*storage.ControlAPILockClient); ok {
		err = apiClient.ClearAllLocks(ctx)
	} else {
		return fmt.Errorf("lock client does not support ClearAllLocks operation")
	}

	if err != nil {
		return fmt.Errorf("failed to clear locks: %w", err)
	}

	fmt.Printf("Successfully cleared all locks\n")
	return nil
}

// buildHealthConfiguration builds comprehensive configuration for health reporting
// with sensitive data masked
func buildHealthConfiguration(conf *config.Config, instanceAPI string) map[string]interface{} {
	maskSensitive := func(value string) string {
		if value == "" {
			return ""
		}
		if len(value) <= 8 {
			return "***"
		}
		return value[:4] + "***" + value[len(value)-4:]
	}

	configMap := map[string]interface{}{
		"service_type":    "bytefreezer-packer",
		"version":         conf.App.Version,
		"instance_api":    instanceAPI,
		"report_interval": conf.HealthReporting.ReportInterval,
		"timeout":         fmt.Sprintf("%ds", conf.HealthReporting.TimeoutSeconds),
		"api": map[string]interface{}{
			"port": conf.Server.ApiPort,
		},
		"control_service": map[string]interface{}{
			"enabled":     conf.ControlService.Enabled,
			"control_url": conf.ControlService.ControlURL,
			"api_key":     maskSensitive(conf.ControlService.APIKey),
			"timeout":     conf.ControlService.TimeoutSeconds,
		},
		"s3_source": map[string]interface{}{
			"bucket_name": conf.S3Source.BucketName,
			"region":      conf.S3Source.Region,
			"endpoint":    conf.S3Source.Endpoint,
			"ssl":         conf.S3Source.Ssl,
			"access_key":  maskSensitive(conf.S3Source.AccessKey),
			"secret_key":  maskSensitive(conf.S3Source.SecretKey),
		},
		"parquet": map[string]interface{}{
			"max_file_size_mb": conf.Parquet.MaxFileSizeMB,
			"timeout_seconds":  conf.Parquet.TimeoutSeconds,
			"compression":      conf.Parquet.Compression,
			"keep_source":      conf.Parquet.KeepSource,
			"streaming_mode":   conf.Parquet.StreamingMode,
		},
		"housekeeping": map[string]interface{}{
			"enabled":          conf.Housekeeping.Enabled,
			"interval_seconds": conf.Housekeeping.IntervalSeconds,
		},
		"capabilities": []string{
			"parquet_processing",
			"metadata_generation",
			"s3_storage",
			"tenant_management",
		},
	}

	// Add account_id at root level for health labeling
	if conf.ControlService.AccountID != "" {
		configMap["account_id"] = conf.ControlService.AccountID
	} else if conf.App.DeploymentType != "" {
		configMap["account_id"] = conf.App.DeploymentType
	}

	return configMap
}

// cleanupStaleOperations marks all in-progress operations for this instance as interrupted
// This allows the system to self-heal on restart - files will be picked up on next processing cycle
func cleanupStaleOperations(cfg *config.Config) {
	if cfg.ControlService.ControlURL == "" {
		log.Debug("Control service URL not configured, skipping operation cleanup")
		return
	}

	cleanupHostname := getDockerContainerID()
	if cleanupHostname == "" {
		cleanupHostname, _ = os.Hostname()
	}
	if cleanupHostname == "" {
		cleanupHostname = "packer-unknown"
	}

	payload := map[string]string{
		"service_type": "packer",
		"instance_id":  cleanupHostname,
	}

	body, err := sonic.Marshal(payload)
	if err != nil {
		log.Warnf("Failed to marshal cleanup request: %v", err)
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("POST", cfg.ControlService.ControlURL+"/api/v1/activity/operations/cleanup", bytes.NewBuffer(body))
	if err != nil {
		log.Warnf("Failed to create cleanup request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	if cfg.ControlService.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+cfg.ControlService.APIKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		log.Warnf("Failed to cleanup stale operations: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		var result struct {
			OperationsCleaned int `json:"operations_cleaned"`
		}
		if err := sonic.ConfigDefault.NewDecoder(resp.Body).Decode(&result); err == nil && result.OperationsCleaned > 0 {
			log.Infof("Cleaned up %d stale operations from previous run", result.OperationsCleaned)
		}
	} else {
		log.Warnf("Failed to cleanup stale operations: HTTP %d", resp.StatusCode)
	}
}

// selfCleanup attempts to remove the service binary and systemd unit (best-effort)
func selfCleanup(serviceName string) {
	// #nosec G204 -- serviceName is always a hardcoded constant from caller
	if err := exec.Command("systemctl", "disable", "--now", serviceName+".service").Run(); err != nil {
		log.Debugf("systemctl disable %s: %v (may not be a systemd service)", serviceName, err)
	}

	unitPath := "/etc/systemd/system/" + serviceName + ".service"
	if err := os.Remove(unitPath); err != nil && !os.IsNotExist(err) {
		log.Debugf("Failed to remove unit file %s: %v", unitPath, err)
	}

	binaryPath, err := os.Executable()
	if err == nil {
		if err := os.Remove(binaryPath); err != nil {
			log.Debugf("Failed to remove binary %s: %v", binaryPath, err)
		}
	}

	log.Infof("Self-cleanup completed for %s", serviceName)
}

func main() {

	err := Run()
	if err != nil {
		log.Fatalf("failed to start: %s\n", err.Error())
		os.Exit(11)
	}
}
