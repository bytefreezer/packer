// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package tenant

import (
	"context"
	"fmt"

	client "github.com/bytefreezer/goodies/control-client"
	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/domain"
)

// AppConfig represents application configuration
type AppConfig struct {
	Name           string `mapstructure:"name"`
	Version        string `mapstructure:"version"`
	DeploymentType string `mapstructure:"deployment_type"` // "managed" or "onprem"
}

// BytefreezerConfig represents bytefreezer configuration
type BytefreezerConfig struct {
	CachePath string `mapstructure:"cachepath"`
}

// ControlServiceConfig represents control service configuration
type ControlServiceConfig struct {
	Enabled        bool   `mapstructure:"enabled"`
	ControlURL     string `mapstructure:"control_url"`
	APIKey         string `mapstructure:"api_key"`
	TimeoutSeconds int    `mapstructure:"timeout_seconds"`
}

// TenantClientConfig represents config needed by TenantClient
type TenantClientConfig struct {
	App            AppConfig            `mapstructure:"app"`
	Bytefreezer    BytefreezerConfig    `mapstructure:"bytefreezer"`
	ControlService ControlServiceConfig `mapstructure:"control_service"`
	Dev            bool                 `mapstructure:"dev"` // Deprecated - use control_service
}

type TenantClient struct {
	config        TenantClientConfig
	controlClient *client.Client
}

func NewTenantClient(conf TenantClientConfig) *TenantClient {
	tc := &TenantClient{
		config: conf,
	}

	// Initialize control client if enabled
	if conf.ControlService.Enabled && conf.ControlService.ControlURL != "" {
		tc.controlClient = client.NewClient(client.Config{
			BaseURL:        conf.ControlService.ControlURL,
			APIKey:         conf.ControlService.APIKey,
			TimeoutSeconds: conf.ControlService.TimeoutSeconds,
		})
		log.Info("Control Service client initialized for tenant management")
	}

	return tc
}

func (tc *TenantClient) FetchTenants() ([]domain.Tenant, error) {
	ctx := context.Background()

	// Priority 1: Try Control Service API (if enabled)
	if tc.controlClient != nil {
		tenants, err := tc.fetchTenantsFromControlService(ctx)
		if err == nil {
			log.Debugf("Successfully fetched %d tenants from Control Service", len(tenants))
			return tenants, nil
		}
		log.Warnf("Failed to fetch tenants from Control Service: %v, falling back", err)
	}

	// Priority 2: Fallback to dev mode / fake data
	if tc.config.Dev {
		log.Warn("Using fake tenant data (dev mode fallback)")
		return tc.getFakeTenants(), nil
	}

	return nil, fmt.Errorf("no tenant data source available: control service and dev mode both unavailable")
}

// fetchTenantsFromControlService fetches tenants from the Control Service
func (tc *TenantClient) fetchTenantsFromControlService(ctx context.Context) ([]domain.Tenant, error) {
	// First, fetch all accounts
	log.Debugf("Fetching all accounts from Control Service")
	accounts, err := tc.controlClient.ListAccounts(ctx, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch accounts: %w", err)
	}

	log.Debugf("Fetched %d accounts from Control Service", len(accounts))

	// Then, fetch all tenants for each account
	allTenants := make([]domain.Tenant, 0)
	for _, account := range accounts {
		if !account.Active {
			log.Debugf("Skipping inactive account: %s (%s)", account.Name, account.ID)
			continue
		}

		// Filter by deployment_type if configured
		if tc.config.App.DeploymentType != "" && account.DeploymentType != tc.config.App.DeploymentType {
			log.Debugf("Skipping account %s (%s) with deployment_type '%s' (service is configured for '%s')",
				account.Name, account.ID, account.DeploymentType, tc.config.App.DeploymentType)
			continue
		}

		// Fetch tenants for this account
		controlTenants, err := tc.controlClient.ListTenants(ctx, account.ID, 1000)
		if err != nil {
			log.Warnf("Failed to fetch tenants for account %s: %v", account.ID, err)
			continue
		}

		// Convert control-client tenants to domain.Tenant
		for _, ct := range controlTenants {
			// Only include active tenants
			if !ct.Active {
				log.Debugf("Skipping inactive tenant: %s (%s)", ct.Name, ct.ID)
				continue
			}

			// Fetch datasets for this tenant
			datasets, err := tc.controlClient.ListDatasets(ctx, ct.ID, 1000)
			if err != nil {
				log.Warnf("Failed to fetch datasets for tenant %s: %v", ct.ID, err)
				// Continue without datasets
			}

			// Build dataset list for domain.Tenant
			domainDatasets := make([]*domain.Dataset, 0, len(datasets))
			for _, ds := range datasets {
				if ds.Active {
					// Parse S3 destination from dataset config
					var s3Dest *domain.S3Destination
					// Handle both "s3" and "minio" types (they use the same S3 API)
					if destConfig, ok := ds.Config["destination"].(map[string]interface{}); ok {
						destType, _ := destConfig["type"].(string)
						if destType == "s3" || destType == "minio" {
							// Extract S3 configuration from destination.connection (consolidated config)
							if connConfig, ok := destConfig["connection"].(map[string]interface{}); ok {
								bucket, _ := connConfig["bucket"].(string)
								region, _ := connConfig["region"].(string)
								endpoint, _ := connConfig["endpoint"].(string)
								ssl, _ := connConfig["ssl"].(bool)
								prefix, _ := connConfig["prefix"].(string)

								var accessKey, secretKey string
								if credsConfig, ok := connConfig["credentials"].(map[string]interface{}); ok {
									accessKey, _ = credsConfig["access_key"].(string)
									secretKey, _ = credsConfig["secret_key"].(string)
								}

								s3Dest = &domain.S3Destination{
									BucketName:   bucket,
									Prefix:       prefix,
									Region:       region,
									AccessKey:    accessKey,
									SecretKey:    secretKey,
									Endpoint:     endpoint,
									Ssl:          ssl,
									Active:       true,
									FailureCount: 0,
									LastError:    "",
								}
							}
						}
					}

					// Extract ProcessingConfig from ds.Config["parquet"]
					var procConfig *domain.ProcessingConfig
					if parquetConfig, ok := ds.Config["parquet"].(map[string]interface{}); ok {
						metadataLevel := "leaf"
						if v, ok := parquetConfig["metadata_level"].(string); ok && v != "" {
							metadataLevel = v
						}

						partitioningScheme := ""
						if v, ok := parquetConfig["partitioning_scheme"].(string); ok {
							partitioningScheme = v
						}

						partitionLayout := ""
						if v, ok := parquetConfig["partition_layout"].(string); ok {
							partitionLayout = v
						}

						procConfig = &domain.ProcessingConfig{
							MetadataLevel:      metadataLevel,
							PartitioningScheme: partitioningScheme,
							PartitionLayout:    partitionLayout,
						}
					}

					domainDatasets = append(domainDatasets, &domain.Dataset{
						ID:               ds.ID,
						Name:             ds.Name,
						TenantID:         ct.ID,
						S3Destination:    s3Dest,
						ProcessingConfig: procConfig,
					})
				}
			}

			allTenants = append(allTenants, domain.Tenant{
				ID:       ct.ID,
				Name:     ct.Name,
				Datasets: domainDatasets,
			})
		}
	}

	log.Debugf("Total tenants fetched from Control Service: %d", len(allTenants))
	return allTenants, nil
}

// getFakeTenants returns fake tenant data for development
func (tc *TenantClient) getFakeTenants() []domain.Tenant {
	return []domain.Tenant{
		{
			ID:   "customer-1",
			Name: "Customer 1 Development Tenant",
			Datasets: []*domain.Dataset{
				{
					ID:       "ebpf-data",
					Name:     "eBPF Network Data",
					TenantID: "customer-1",
					S3Destination: &domain.S3Destination{
						BucketName:   "packer",
						Prefix:       "ebpf-data/",
						Region:       "us-east-1",
						AccessKey:    "AKIAQNXBAHUKIMJP26P6",            // TODO: Move to env vars/secrets manager
						SecretKey:    "SECRET_KEY_AKIAQNXBAHUKIMJP26P6", // TODO: Move to env vars/secrets manager
						Endpoint:     "192.168.86.125:9000",
						Ssl:          false,
						Active:       true,
						FailureCount: 0,
						LastError:    "",
					},
					ProcessingConfig: &domain.ProcessingConfig{
						FlattenJSON:        false,
						FlattenDelimiter:   "_",
						EnableRawStorage:   true,
						PartitioningScheme: "hive",
						PartitionLayout:    "tenant={{.TenantID}}/dataset={{.DatasetID}}/year={{.Year}}/month={{.Month}}/day={{.Day}}/",
					},
				},
				{
					ID:       "sflow-data",
					Name:     "sFlow Network Data",
					TenantID: "customer-1",
					S3Destination: &domain.S3Destination{
						BucketName:   "packer",
						Prefix:       "sflow-data/",
						Region:       "us-east-1",
						AccessKey:    "AKIAQNXBAHUKIMJP26P6",            // TODO: Move to env vars/secrets manager
						SecretKey:    "SECRET_KEY_AKIAQNXBAHUKIMJP26P6", // TODO: Move to env vars/secrets manager
						Endpoint:     "192.168.86.125:9000",
						Ssl:          false,
						Active:       true,
						FailureCount: 0,
						LastError:    "",
					},
					ProcessingConfig: &domain.ProcessingConfig{
						FlattenJSON:        true,
						FlattenDelimiter:   "_",
						EnableRawStorage:   false,
						PartitioningScheme: "date",
						PartitionLayout:    "{{.TenantID}}/{{.DatasetID}}/{{.Year}}/{{.Month}}/{{.Day}}/",
					},
				},
			},
		},
	}
}

func (tc *TenantClient) GetTenantByID(tenants []domain.Tenant, tenantID string) *domain.Tenant {
	for _, tenant := range tenants {
		if tenant.ID == tenantID {
			return &tenant
		}
	}
	return nil
}
