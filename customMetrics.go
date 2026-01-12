package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"time"
)

// MetricSource is the interface that all custom metric sources must implement
type MetricSource interface {
	Start() error
	Stop() error
	GetStatus() SourceStatus
	GetName() string
	GetType() string
}

// SourceStatus represents the current status of a metric source
type SourceStatus struct {
	Name        string            `json:"name"`
	Type        string            `json:"type"`
	Enabled     bool              `json:"enabled"`
	Running     bool              `json:"running"`
	LastUpdate  time.Time         `json:"last_update"`
	LastError   string            `json:"last_error,omitempty"`
	DataKeys    []string          `json:"data_keys"`
	Stats       SourceStats       `json:"stats"`
	CustomInfo  map[string]string `json:"custom_info,omitempty"`
}

// SourceStats contains statistics for a metric source
type SourceStats struct {
	SuccessCount int64     `json:"success_count"`
	ErrorCount   int64     `json:"error_count"`
	StartTime    time.Time `json:"start_time"`
	LastDuration int64     `json:"last_duration_ms"`
}

// CustomMetricsConfig is the top-level configuration for custom metrics
type CustomMetricsConfig struct {
	Sources        []SourceConfig      `json:"sources"`
	GlobalSettings GlobalMetricSettings `json:"global_settings,omitempty"`
}

// GlobalMetricSettings contains global configuration for all metric sources
type GlobalMetricSettings struct {
	EnableLogging      bool `json:"enable_logging"`
	ErrorRetryInterval int  `json:"error_retry_interval"` // seconds
	MaxRetries         int  `json:"max_retries"`
}

// SourceConfig is the configuration for a single metric source
type SourceConfig struct {
	Type    string                 `json:"type"`    // "http_endpoint", "command", "env", "json_file"
	Name    string                 `json:"name"`
	Enabled bool                   `json:"enabled"`
	Config  map[string]interface{} `json:"config"`
}

// CustomMetricManager manages all custom metric sources
type CustomMetricManager struct {
	sources        []MetricSource
	config         CustomMetricsConfig
	stopChan       chan struct{}
	wg             sync.WaitGroup
	mu             sync.RWMutex
	running        bool
}

// NewCustomMetricManager creates a new custom metric manager
func NewCustomMetricManager(config CustomMetricsConfig) (*CustomMetricManager, error) {
	manager := &CustomMetricManager{
		config:   config,
		sources:  make([]MetricSource, 0),
		stopChan: make(chan struct{}),
	}

	// Create sources based on configuration
	for _, sourceConfig := range config.Sources {
		if !sourceConfig.Enabled {
			log.Printf("CustomMetrics: Source '%s' is disabled, skipping", sourceConfig.Name)
			continue
		}

		source, err := createSource(sourceConfig, config.GlobalSettings)
		if err != nil {
			log.Printf("CustomMetrics: Failed to create source '%s': %v", sourceConfig.Name, err)
			continue
		}

		manager.sources = append(manager.sources, source)
	}

	return manager, nil
}

// createSource creates a metric source based on the configuration
func createSource(config SourceConfig, globalSettings GlobalMetricSettings) (MetricSource, error) {
	switch config.Type {
	case "http_endpoint":
		return NewHTTPSource(config, globalSettings)
	case "command":
		return NewCommandSource(config, globalSettings)
	case "env":
		return NewEnvVarSource(config, globalSettings)
	case "json_file":
		return NewJSONFileSource(config, globalSettings)
	default:
		return nil, fmt.Errorf("unknown source type: %s", config.Type)
	}
}

// Start starts all metric sources
func (m *CustomMetricManager) Start() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("manager already running")
	}

	log.Printf("CustomMetrics: Starting %d metric sources", len(m.sources))

	for _, source := range m.sources {
		if err := source.Start(); err != nil {
			log.Printf("CustomMetrics: Failed to start source '%s': %v", source.GetName(), err)
		}
	}

	m.running = true
	return nil
}

// Stop stops all metric sources
func (m *CustomMetricManager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return nil
	}

	log.Printf("CustomMetrics: Stopping %d metric sources", len(m.sources))

	close(m.stopChan)

	for _, source := range m.sources {
		if err := source.Stop(); err != nil {
			log.Printf("CustomMetrics: Failed to stop source '%s': %v", source.GetName(), err)
		}
	}

	m.wg.Wait()
	m.running = false

	return nil
}

// GetAllStatus returns the status of all sources
func (m *CustomMetricManager) GetAllStatus() []SourceStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statuses := make([]SourceStatus, 0, len(m.sources))
	for _, source := range m.sources {
		statuses = append(statuses, source.GetStatus())
	}

	return statuses
}

// GetSourceByName returns a source by name
func (m *CustomMetricManager) GetSourceByName(name string) MetricSource {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, source := range m.sources {
		if source.GetName() == name {
			return source
		}
	}

	return nil
}

// ============================================================================
// HTTP Source - Accepts data via HTTP POST
// ============================================================================

// HTTPSource accepts custom metric data via HTTP POST
type HTTPSource struct {
	name           string
	enabled        bool
	allowedKeys    []string
	globalSettings GlobalMetricSettings
	mu             sync.RWMutex
	status         SourceStatus
}

// HTTPSourceConfig is the configuration for HTTP source
type HTTPSourceConfig struct {
	AllowedKeys []string `json:"allowed_keys"`
}

// NewHTTPSource creates a new HTTP source
func NewHTTPSource(config SourceConfig, globalSettings GlobalMetricSettings) (*HTTPSource, error) {
	var httpConfig HTTPSourceConfig
	configBytes, _ := json.Marshal(config.Config)
	if err := json.Unmarshal(configBytes, &httpConfig); err != nil {
		return nil, fmt.Errorf("invalid HTTP source config: %v", err)
	}

	source := &HTTPSource{
		name:           config.Name,
		enabled:        config.Enabled,
		allowedKeys:    httpConfig.AllowedKeys,
		globalSettings: globalSettings,
		status: SourceStatus{
			Name:    config.Name,
			Type:    "http_endpoint",
			Enabled: config.Enabled,
			Running: false,
			DataKeys: httpConfig.AllowedKeys,
		},
	}

	return source, nil
}

func (s *HTTPSource) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status.Running = true
	s.status.Stats.StartTime = time.Now()

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: HTTPSource '%s' started (passive mode)", s.name)
	}

	return nil
}

func (s *HTTPSource) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status.Running = false

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: HTTPSource '%s' stopped", s.name)
	}

	return nil
}

func (s *HTTPSource) GetStatus() SourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *HTTPSource) GetName() string {
	return s.name
}

func (s *HTTPSource) GetType() string {
	return "http_endpoint"
}

// UpdateData updates data from HTTP POST (called by HTTP handler)
func (s *HTTPSource) UpdateData(data map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Validate keys if allowedKeys is set
	if len(s.allowedKeys) > 0 {
		for key := range data {
			allowed := false
			for _, allowedKey := range s.allowedKeys {
				if key == allowedKey {
					allowed = true
					break
				}
			}
			if !allowed {
				continue // Skip non-allowed keys
			}
			globalData.Store(key, fmt.Sprint(data[key]))
		}
	} else {
		// No restrictions, store all keys
		for key, value := range data {
			globalData.Store(key, fmt.Sprint(value))
		}
	}

	s.status.LastUpdate = time.Now()
	s.status.Stats.SuccessCount++

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: HTTPSource '%s' updated %d keys", s.name, len(data))
	}

	return nil
}

// ============================================================================
// Command Source - Executes shell commands periodically
// ============================================================================

// CommandSource executes shell commands and stores the output
type CommandSource struct {
	name           string
	command        string
	interval       int
	timeout        int
	parser         string
	dataKey        string
	enabled        bool
	globalSettings GlobalMetricSettings
	stopChan       chan struct{}
	mu             sync.RWMutex
	status         SourceStatus
}

// CommandSourceConfig is the configuration for command source
type CommandSourceConfig struct {
	Command  string `json:"command"`
	Interval int    `json:"interval"` // seconds
	Timeout  int    `json:"timeout"`  // seconds
	Parser   string `json:"parser"`   // "stdout", "json:path", "line:N", "regex:pattern"
	DataKey  string `json:"data_key"`
}

// NewCommandSource creates a new command source
func NewCommandSource(config SourceConfig, globalSettings GlobalMetricSettings) (*CommandSource, error) {
	var cmdConfig CommandSourceConfig
	configBytes, _ := json.Marshal(config.Config)
	if err := json.Unmarshal(configBytes, &cmdConfig); err != nil {
		return nil, fmt.Errorf("invalid command source config: %v", err)
	}

	if cmdConfig.Command == "" {
		return nil, fmt.Errorf("command is required")
	}
	if cmdConfig.DataKey == "" {
		return nil, fmt.Errorf("data_key is required")
	}
	if cmdConfig.Interval < 1 {
		cmdConfig.Interval = 5
	}
	if cmdConfig.Timeout < 1 {
		cmdConfig.Timeout = 5
	}
	if cmdConfig.Parser == "" {
		cmdConfig.Parser = "stdout"
	}

	source := &CommandSource{
		name:           config.Name,
		command:        cmdConfig.Command,
		interval:       cmdConfig.Interval,
		timeout:        cmdConfig.Timeout,
		parser:         cmdConfig.Parser,
		dataKey:        cmdConfig.DataKey,
		enabled:        config.Enabled,
		globalSettings: globalSettings,
		stopChan:       make(chan struct{}),
		status: SourceStatus{
			Name:     config.Name,
			Type:     "command",
			Enabled:  config.Enabled,
			Running:  false,
			DataKeys: []string{cmdConfig.DataKey},
			CustomInfo: map[string]string{
				"command":  cmdConfig.Command,
				"interval": fmt.Sprintf("%ds", cmdConfig.Interval),
				"parser":   cmdConfig.Parser,
			},
		},
	}

	return source, nil
}

func (s *CommandSource) Start() error {
	s.mu.Lock()
	s.status.Running = true
	s.status.Stats.StartTime = time.Now()
	s.mu.Unlock()

	go s.run()

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: CommandSource '%s' started (interval: %ds)", s.name, s.interval)
	}

	return nil
}

func (s *CommandSource) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.status.Running {
		return nil
	}

	close(s.stopChan)
	s.status.Running = false

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: CommandSource '%s' stopped", s.name)
	}

	return nil
}

func (s *CommandSource) run() {
	// Execute immediately on start
	s.executeCommand()

	ticker := time.NewTicker(time.Duration(s.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.executeCommand()
		}
	}
}

func (s *CommandSource) executeCommand() {
	startTime := time.Now()

	// Log execution start
	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: CommandSource '%s' executing: %s", s.name, s.command)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(s.timeout)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "bash", "-c", s.command)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	duration := time.Since(startTime).Milliseconds()

	s.mu.Lock()
	s.status.Stats.LastDuration = duration
	s.mu.Unlock()

	if err != nil {
		s.mu.Lock()
		s.status.Stats.ErrorCount++
		if ctx.Err() == context.DeadlineExceeded {
			s.status.LastError = fmt.Sprintf("timeout after %ds", s.timeout)
			globalData.Store(s.dataKey, "TIMEOUT")
		} else {
			s.status.LastError = fmt.Sprintf("%v: %s", err, stderr.String())
			globalData.Store(s.dataKey, "ERROR")
		}
		s.mu.Unlock()

		if s.globalSettings.EnableLogging {
			log.Printf("CustomMetrics: CommandSource '%s' failed: %s", s.name, s.status.LastError)
		}
		return
	}

	// Parse output
	result, err := parseCommandOutput(stdout.String(), s.parser)
	if err != nil {
		s.mu.Lock()
		s.status.Stats.ErrorCount++
		s.status.LastError = fmt.Sprintf("parse error: %v", err)
		s.mu.Unlock()

		globalData.Store(s.dataKey, "PARSE_ERROR")

		if s.globalSettings.EnableLogging {
			log.Printf("CustomMetrics: CommandSource '%s' parse error: %v", s.name, err)
		}
		return
	}

	// Store result
	globalData.Store(s.dataKey, result)

	s.mu.Lock()
	s.status.LastUpdate = time.Now()
	s.status.Stats.SuccessCount++
	s.status.LastError = ""
	s.mu.Unlock()

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: CommandSource '%s' result: %s = %s (took %dms)",
			s.name, s.dataKey, result, duration)
	}
}

func (s *CommandSource) GetStatus() SourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *CommandSource) GetName() string {
	return s.name
}

func (s *CommandSource) GetType() string {
	return "command"
}

// ExecuteNow triggers an immediate execution (for API calls)
func (s *CommandSource) ExecuteNow() {
	go s.executeCommand()
}

// ============================================================================
// Environment Variable Source
// ============================================================================

// EnvVarSource reads environment variables
type EnvVarSource struct {
	name            string
	variables       []EnvVarMapping
	refreshInterval int
	enabled         bool
	globalSettings  GlobalMetricSettings
	stopChan        chan struct{}
	mu              sync.RWMutex
	status          SourceStatus
}

// EnvVarMapping maps an environment variable to a data key
type EnvVarMapping struct {
	EnvVar  string `json:"env_var"`
	DataKey string `json:"data_key"`
	Default string `json:"default"`
}

// EnvVarSourceConfig is the configuration for env var source
type EnvVarSourceConfig struct {
	Variables       []EnvVarMapping `json:"variables"`
	RefreshInterval int             `json:"refresh_interval"` // seconds, 0 = no refresh
}

// NewEnvVarSource creates a new environment variable source
func NewEnvVarSource(config SourceConfig, globalSettings GlobalMetricSettings) (*EnvVarSource, error) {
	var envConfig EnvVarSourceConfig
	configBytes, _ := json.Marshal(config.Config)
	if err := json.Unmarshal(configBytes, &envConfig); err != nil {
		return nil, fmt.Errorf("invalid env var source config: %v", err)
	}

	if len(envConfig.Variables) == 0 {
		return nil, fmt.Errorf("no variables configured")
	}

	dataKeys := make([]string, len(envConfig.Variables))
	for i, v := range envConfig.Variables {
		dataKeys[i] = v.DataKey
	}

	source := &EnvVarSource{
		name:            config.Name,
		variables:       envConfig.Variables,
		refreshInterval: envConfig.RefreshInterval,
		enabled:         config.Enabled,
		globalSettings:  globalSettings,
		stopChan:        make(chan struct{}),
		status: SourceStatus{
			Name:     config.Name,
			Type:     "env",
			Enabled:  config.Enabled,
			Running:  false,
			DataKeys: dataKeys,
			CustomInfo: map[string]string{
				"variable_count": fmt.Sprintf("%d", len(envConfig.Variables)),
				"refresh":        fmt.Sprintf("%ds", envConfig.RefreshInterval),
			},
		},
	}

	return source, nil
}

func (s *EnvVarSource) Start() error {
	s.mu.Lock()
	s.status.Running = true
	s.status.Stats.StartTime = time.Now()
	s.mu.Unlock()

	// Read immediately
	s.readEnvironmentVariables()

	// Set up periodic refresh if configured
	if s.refreshInterval > 0 {
		go s.run()
	}

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: EnvVarSource '%s' started (%d variables, refresh: %ds)",
			s.name, len(s.variables), s.refreshInterval)
	}

	return nil
}

func (s *EnvVarSource) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.status.Running {
		return nil
	}

	if s.refreshInterval > 0 {
		close(s.stopChan)
	}
	s.status.Running = false

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: EnvVarSource '%s' stopped", s.name)
	}

	return nil
}

func (s *EnvVarSource) run() {
	ticker := time.NewTicker(time.Duration(s.refreshInterval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.readEnvironmentVariables()
		}
	}
}

func (s *EnvVarSource) readEnvironmentVariables() {
	startTime := time.Now()

	for _, variable := range s.variables {
		value := os.Getenv(variable.EnvVar)
		if value == "" && variable.Default != "" {
			value = variable.Default
		}

		globalData.Store(variable.DataKey, value)
	}

	duration := time.Since(startTime).Milliseconds()

	s.mu.Lock()
	s.status.LastUpdate = time.Now()
	s.status.Stats.SuccessCount++
	s.status.Stats.LastDuration = duration
	s.mu.Unlock()

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: EnvVarSource '%s' read %d variables", s.name, len(s.variables))
	}
}

func (s *EnvVarSource) GetStatus() SourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *EnvVarSource) GetName() string {
	return s.name
}

func (s *EnvVarSource) GetType() string {
	return "env"
}

// ============================================================================
// JSON File Source
// ============================================================================

// JSONFileSource monitors JSON files for custom metrics
type JSONFileSource struct {
	name           string
	filePath       string
	interval       int
	mappings       []JSONMapping
	enabled        bool
	globalSettings GlobalMetricSettings
	stopChan       chan struct{}
	mu             sync.RWMutex
	status         SourceStatus
}

// JSONMapping maps a JSON path to a data key
type JSONMapping struct {
	JSONPath string `json:"json_path"`
	DataKey  string `json:"data_key"`
}

// JSONFileSourceConfig is the configuration for JSON file source
type JSONFileSourceConfig struct {
	Path     string        `json:"path"`
	Interval int           `json:"interval"` // seconds
	Mappings []JSONMapping `json:"mappings"`
}

// NewJSONFileSource creates a new JSON file source
func NewJSONFileSource(config SourceConfig, globalSettings GlobalMetricSettings) (*JSONFileSource, error) {
	var jsonConfig JSONFileSourceConfig
	configBytes, _ := json.Marshal(config.Config)
	if err := json.Unmarshal(configBytes, &jsonConfig); err != nil {
		return nil, fmt.Errorf("invalid JSON file source config: %v", err)
	}

	if jsonConfig.Path == "" {
		return nil, fmt.Errorf("file path is required")
	}
	if len(jsonConfig.Mappings) == 0 {
		return nil, fmt.Errorf("no mappings configured")
	}
	if jsonConfig.Interval < 1 {
		jsonConfig.Interval = 5
	}

	dataKeys := make([]string, len(jsonConfig.Mappings))
	for i, m := range jsonConfig.Mappings {
		dataKeys[i] = m.DataKey
	}

	source := &JSONFileSource{
		name:           config.Name,
		filePath:       jsonConfig.Path,
		interval:       jsonConfig.Interval,
		mappings:       jsonConfig.Mappings,
		enabled:        config.Enabled,
		globalSettings: globalSettings,
		stopChan:       make(chan struct{}),
		status: SourceStatus{
			Name:     config.Name,
			Type:     "json_file",
			Enabled:  config.Enabled,
			Running:  false,
			DataKeys: dataKeys,
			CustomInfo: map[string]string{
				"file":     jsonConfig.Path,
				"interval": fmt.Sprintf("%ds", jsonConfig.Interval),
				"mappings": fmt.Sprintf("%d", len(jsonConfig.Mappings)),
			},
		},
	}

	return source, nil
}

func (s *JSONFileSource) Start() error {
	s.mu.Lock()
	s.status.Running = true
	s.status.Stats.StartTime = time.Now()
	s.mu.Unlock()

	go s.run()

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: JSONFileSource '%s' started (file: %s, interval: %ds)",
			s.name, s.filePath, s.interval)
	}

	return nil
}

func (s *JSONFileSource) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.status.Running {
		return nil
	}

	close(s.stopChan)
	s.status.Running = false

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: JSONFileSource '%s' stopped", s.name)
	}

	return nil
}

func (s *JSONFileSource) run() {
	// Read immediately on start
	s.readJSONFile()

	ticker := time.NewTicker(time.Duration(s.interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopChan:
			return
		case <-ticker.C:
			s.readJSONFile()
		}
	}
}

func (s *JSONFileSource) readJSONFile() {
	startTime := time.Now()

	// Read file
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		s.mu.Lock()
		s.status.Stats.ErrorCount++
		s.status.LastError = fmt.Sprintf("read error: %v", err)
		s.mu.Unlock()

		for _, mapping := range s.mappings {
			globalData.Store(mapping.DataKey, "FILE_ERROR")
		}

		if s.globalSettings.EnableLogging {
			log.Printf("CustomMetrics: JSONFileSource '%s' read error: %v", s.name, err)
		}
		return
	}

	// Parse JSON
	var jsonData interface{}
	if err := json.Unmarshal(data, &jsonData); err != nil {
		s.mu.Lock()
		s.status.Stats.ErrorCount++
		s.status.LastError = fmt.Sprintf("JSON parse error: %v", err)
		s.mu.Unlock()

		for _, mapping := range s.mappings {
			globalData.Store(mapping.DataKey, "PARSE_ERROR")
		}

		if s.globalSettings.EnableLogging {
			log.Printf("CustomMetrics: JSONFileSource '%s' parse error: %v", s.name, err)
		}
		return
	}

	// Extract values for each mapping
	successCount := 0
	for _, mapping := range s.mappings {
		value, err := extractJSONPath(string(data), mapping.JSONPath)
		if err != nil {
			globalData.Store(mapping.DataKey, "EXTRACT_ERROR")
			if s.globalSettings.EnableLogging {
				log.Printf("CustomMetrics: JSONFileSource '%s' extract error for %s: %v",
					s.name, mapping.JSONPath, err)
			}
		} else {
			globalData.Store(mapping.DataKey, value)
			successCount++
		}
	}

	duration := time.Since(startTime).Milliseconds()

	s.mu.Lock()
	s.status.LastUpdate = time.Now()
	s.status.Stats.SuccessCount++
	s.status.Stats.LastDuration = duration
	s.status.LastError = ""
	s.mu.Unlock()

	if s.globalSettings.EnableLogging {
		log.Printf("CustomMetrics: JSONFileSource '%s' updated %d/%d mappings (took %dms)",
			s.name, successCount, len(s.mappings), duration)
	}
}

func (s *JSONFileSource) GetStatus() SourceStatus {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.status
}

func (s *JSONFileSource) GetName() string {
	return s.name
}

func (s *JSONFileSource) GetType() string {
	return "json_file"
}

// ============================================================================
// Parsing Utilities
// ============================================================================

// parseCommandOutput parses command output based on the parser type
func parseCommandOutput(output, parser string) (string, error) {
	output = strings.TrimSpace(output)

	if parser == "stdout" {
		return output, nil
	}

	// JSON path parser: "json:path.to.key"
	if strings.HasPrefix(parser, "json:") {
		path := strings.TrimPrefix(parser, "json:")
		return extractJSONPath(output, path)
	}

	// Line parser: "line:N"
	if strings.HasPrefix(parser, "line:") {
		var lineNum int
		_, err := fmt.Sscanf(parser, "line:%d", &lineNum)
		if err != nil {
			return "", fmt.Errorf("invalid line parser format")
		}
		lines := strings.Split(output, "\n")
		if lineNum >= 0 && lineNum < len(lines) {
			return strings.TrimSpace(lines[lineNum]), nil
		}
		return "", fmt.Errorf("line %d out of range (total: %d lines)", lineNum, len(lines))
	}

	// Regex parser: "regex:pattern"
	if strings.HasPrefix(parser, "regex:") {
		pattern := strings.TrimPrefix(parser, "regex:")
		re, err := regexp.Compile(pattern)
		if err != nil {
			return "", fmt.Errorf("invalid regex: %v", err)
		}
		matches := re.FindStringSubmatch(output)
		if len(matches) > 1 {
			return matches[1], nil
		}
		return "", fmt.Errorf("no regex match found")
	}

	return "", fmt.Errorf("unknown parser type: %s", parser)
}

// extractJSONPath extracts a value from JSON using dot notation path
func extractJSONPath(jsonStr, path string) (string, error) {
	var data interface{}
	err := json.Unmarshal([]byte(jsonStr), &data)
	if err != nil {
		return "", fmt.Errorf("invalid JSON: %v", err)
	}

	keys := strings.Split(path, ".")
	current := data

	for _, key := range keys {
		switch v := current.(type) {
		case map[string]interface{}:
			var ok bool
			current, ok = v[key]
			if !ok {
				return "", fmt.Errorf("key '%s' not found in JSON", key)
			}
		default:
			return "", fmt.Errorf("cannot traverse path at key '%s'", key)
		}
	}

	if current == nil {
		return "", nil
	}

	return fmt.Sprint(current), nil
}
