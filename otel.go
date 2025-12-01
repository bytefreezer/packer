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
	"go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// InitOtelProvider initializes the OpenTelemetry provider with multi-mode support
func InitOtelProvider(conf *config.Config) func() {
	if !conf.Otel.Enabled {
		return nil
	}

	ctx := context.Background()

	// Create a resource
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(conf.App.Name),
			semconv.ServiceVersion(conf.App.Version),
		),
	)
	if err != nil {
		log.Errorf("Failed to create OpenTelemetry resource: %v", err)
		return nil
	}

	var meterProvider *sdkmetric.MeterProvider
	var httpServer *http.Server
	var pushTicker *time.Ticker

	// Prometheus metrics server mode
	prometheusExporter, err := prometheus.New()
	if err != nil {
		log.Errorf("Failed to create Prometheus exporter: %v", err)
		return nil
	}

	// Create metric provider with Prometheus reader
	meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(prometheusExporter),
	)

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)

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

	// Return shutdown function
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Stop push ticker if running
		if pushTicker != nil {
			pushTicker.Stop()
		}

		// Shutdown HTTP server if running
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
