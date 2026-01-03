// Licensed under Elastic License 2.0
// See LICENSE.txt for details

package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/bytefreezer/goodies/log"
	"github.com/bytefreezer/packer/config"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// InitOtelProvider initializes the OpenTelemetry provider with multi-mode support
// Modes:
//   - "prometheus": Expose /metrics endpoint for Prometheus to scrape (pull)
//   - "otlp_http": Push metrics to Prometheus OTLP receiver via HTTP
//   - "otlp_grpc": Push metrics to OpenTelemetry Collector via gRPC
func InitOtelProvider(conf *config.Config) func() {
	if !conf.Otel.Enabled {
		log.Info("OpenTelemetry metrics disabled")
		return nil
	}

	ctx := context.Background()

	// Determine service name
	serviceName := conf.Otel.ServiceName
	if serviceName == "" {
		serviceName = conf.App.Name
	}

	// Create a resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(conf.App.Version),
		),
	)
	if err != nil {
		log.Errorf("Failed to create OpenTelemetry resource: %v", err)
		return nil
	}

	var meterProvider *sdkmetric.MeterProvider
	var httpServer *http.Server

	mode := conf.Otel.Mode
	if mode == "" {
		mode = "prometheus" // Default to prometheus for backwards compatibility
	}

	switch mode {
	case "prometheus":
		// Prometheus pull mode - expose /metrics endpoint
		prometheusExporter, err := prometheus.New()
		if err != nil {
			log.Errorf("Failed to create Prometheus exporter: %v", err)
			return nil
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(prometheusExporter),
		)

		// Start HTTP server for metrics endpoint
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		httpServer = &http.Server{
			Addr:              fmt.Sprintf("%s:%d", conf.Otel.MetricsHost, conf.Otel.MetricsPort),
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			ReadTimeout:       30 * time.Second,
			WriteTimeout:      30 * time.Second,
			IdleTimeout:       120 * time.Second,
		}

		go func() {
			log.Infof("Starting Prometheus metrics server on %s", httpServer.Addr)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Errorf("Failed to start metrics server: %v", err)
			}
		}()

		log.Infof("OpenTelemetry initialized in Prometheus mode on %s:%d/metrics",
			conf.Otel.MetricsHost, conf.Otel.MetricsPort)

	case "otlp_http":
		// OTLP HTTP push mode - push to Prometheus OTLP receiver
		if conf.Otel.OTLPEndpoint == "" {
			log.Errorf("OTLP HTTP mode requires otlp_endpoint to be set")
			return nil
		}

		pushInterval := time.Duration(conf.Otel.PushIntervalSeconds) * time.Second
		if pushInterval <= 0 {
			pushInterval = 15 * time.Second
		}

		exporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(conf.Otel.OTLPEndpoint),
			otlpmetrichttp.WithInsecure(),
		)
		if err != nil {
			log.Errorf("Failed to create OTLP HTTP exporter: %v", err)
			return nil
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(pushInterval),
			)),
		)

		log.Infof("OpenTelemetry initialized in OTLP HTTP push mode to %s (interval: %v)",
			conf.Otel.OTLPEndpoint, pushInterval)

	case "otlp_grpc":
		// OTLP gRPC push mode - push to OpenTelemetry Collector
		if conf.Otel.OTLPEndpoint == "" {
			log.Errorf("OTLP gRPC mode requires otlp_endpoint to be set")
			return nil
		}

		pushInterval := time.Duration(conf.Otel.PushIntervalSeconds) * time.Second
		if pushInterval <= 0 {
			pushInterval = 15 * time.Second
		}

		exporter, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(conf.Otel.OTLPEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			log.Errorf("Failed to create OTLP gRPC exporter: %v", err)
			return nil
		}

		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				exporter,
				sdkmetric.WithInterval(pushInterval),
			)),
		)

		log.Infof("OpenTelemetry initialized in OTLP gRPC push mode to %s (interval: %v)",
			conf.Otel.OTLPEndpoint, pushInterval)

	default:
		log.Errorf("Unknown monitoring mode: %s (valid: prometheus, otlp_http, otlp_grpc)", mode)
		return nil
	}

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)

	// Return shutdown function
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Shutdown HTTP server if running (prometheus mode)
		if httpServer != nil {
			if err := httpServer.Shutdown(ctx); err != nil {
				log.Errorf("Failed to shutdown metrics server: %v", err)
			}
		}

		// Shutdown meter provider
		if err := meterProvider.Shutdown(ctx); err != nil {
			log.Errorf("Failed to shutdown OpenTelemetry meter provider: %v", err)
		}
	}
}
