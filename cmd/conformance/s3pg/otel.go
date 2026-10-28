// Copyright 2026 The Tessera authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"os"

	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

// initOTel initialises the open telemetry support for metrics and tracing.
//
// Unlike cmd/conformance/aws, which assumes an ADOT collector on localhost and AWS resource
// detectors, this is vendor-neutral: export over OTLP/gRPC is enabled only if one of the
// OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
// environment variables is set (e.g. OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317), and
// the exporters are otherwise configured with the standard OTEL_* environment variables.
//
// Tracing is enabled with statistical sampling, with the probability passed in.
// Returns a shutdown function which should be called just before exiting the process.
func initOTel(ctx context.Context, traceFraction float64) func(context.Context) {
	var shutdownFuncs []func(context.Context) error
	// shutdown combines shutdown functions from multiple OpenTelemetry
	// components into a single function.
	shutdown := func(ctx context.Context) {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		if err != nil {
			slog.ErrorContext(ctx, "OTel shutdown", slog.Any("error", err))
		}
	}

	configured := false
	for _, e := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"} {
		if os.Getenv(e) != "" {
			configured = true
		}
	}
	if !configured || os.Getenv("OTEL_SDK_DISABLED") == "true" {
		slog.InfoContext(ctx, "OpenTelemetry export disabled; set OTEL_EXPORTER_OTLP_ENDPOINT to enable it")
		return shutdown
	}

	resources, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		// Add your own custom attributes to identify your application
		resource.WithAttributes(
			semconv.ServiceNameKey.String("conformance"),
			semconv.ServiceNamespaceKey.String("tessera"),
		),
		resource.WithFromEnv(), // unpacks OTEL_RESOURCE_ATTRIBUTES and OTEL_SERVICE_NAME
	)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to detect resources", slog.Any("error", err))
		os.Exit(1)
	}

	// Create and start new OTLP metric exporter
	metricExporter, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create new OTLP metric exporter", slog.Any("error", err))
		os.Exit(1)
	}
	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter)),
		metric.WithResource(resources),
	)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)
	otel.SetMeterProvider(mp)

	// Create and start new OTLP trace exporter
	traceExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		slog.ErrorContext(ctx, "Failed to create new OTLP trace exporter", slog.Any("error", err))
		os.Exit(1)
	}

	tp := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(resources),
		trace.WithSampler(trace.TraceIDRatioBased(traceFraction)),
	)
	shutdownFuncs = append(shutdownFuncs, tp.Shutdown)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	return shutdown
}
