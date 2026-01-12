package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/png"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gofiber/fiber/v2"
)

// validateJSON validates that input is proper JSON and doesn't contain suspicious content
func validateJSON(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty JSON data")
	}
	if len(data) > 10*1024*1024 { // 10MB limit
		return fmt.Errorf("JSON data too large")
	}
	
	// Basic JSON validation
	var temp interface{}
	if err := json.Unmarshal(data, &temp); err != nil {
		return fmt.Errorf("invalid JSON: %w", err)
	}
	
	// Check for potentially malicious patterns
	dataStr := string(data)
	suspiciousPatterns := []string{
		"<script", "</script", "javascript:", "eval(", "document.", "window.",
	}
	for _, pattern := range suspiciousPatterns {
		if strings.Contains(strings.ToLower(dataStr), pattern) {
			return fmt.Errorf("suspicious content detected")
		}
	}
	
	return nil
}

// secureUnmarshal safely unmarshals JSON with validation
func secureUnmarshal(data []byte, v interface{}) error {
	if err := validateJSON(data); err != nil {
		return err
	}
	return json.Unmarshal(data, v)
}

var (
	drawMu               sync.Mutex
	webFrame             *image.RGBA
	configMutex          sync.RWMutex
	defaultConfig        Config                 // loaded from default_config.json
	userOverrides        map[string]interface{} // raw overrides from user_config.json
	userJsonConfig       = ""

	// Runtime brightness override system
	runtimeBrightnessMu   sync.RWMutex
	runtimeMaxBrightness  *int   // nil = use config.json, non-nil = override value
)

func serveFrame(c *fiber.Ctx) error {
	var err error
	var buf bytes.Buffer

	if webFrame == nil {
		webFrame = GetFrameBuffer(PCAT2_LCD_WIDTH, PCAT2_LCD_HEIGHT)
		clearFrame(webFrame, PCAT2_LCD_WIDTH, PCAT2_LCD_HEIGHT)
	}

	frameMutex.RLock()
	// Use legacy framebuffers that are actively being rendered to
	var topBuffer, middleBuffer, footerBuffer *image.RGBA
	topBuffer = getTopBarFramebuffer(0)
	middleBuffer = getMiddleFramebuffer(0)
	footerBuffer = getFooterFramebuffer(frames%2)

	// Safety checks for nil buffers
	if topBuffer == nil {
		frameMutex.RUnlock()
		log.Printf("⚠️ HTTP serveFrame: topBuffer is nil")
		return c.Status(fiber.StatusServiceUnavailable).SendString("Top bar frame buffer not available")
	}
	if middleBuffer == nil {
		frameMutex.RUnlock()
		log.Printf("⚠️ HTTP serveFrame: middleBuffer is nil")
		return c.Status(fiber.StatusServiceUnavailable).SendString("Middle frame buffer not available")
	}
	if footerBuffer == nil {
		frameMutex.RUnlock()
		log.Printf("⚠️ HTTP serveFrame: footerBuffer is nil")
		return c.Status(fiber.StatusServiceUnavailable).SendString("Footer frame buffer not available")
	}

	// Copy frame buffers with proper bounds - each buffer has its own dimensions
	// Top bar: 172×32 at y=0
	err = copyImageToImageAt(webFrame, topBuffer, 0, 0)
	if err != nil {
		log.Printf("❌ HTTP serveFrame: Failed to copy top bar frame (172×32 at y=0): %v", err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to copy top bar frame: " + err.Error())
	}

	// Middle: 172×266 at y=32  
	err = copyImageToImageAt(webFrame, middleBuffer, 0, PCAT2_TOP_BAR_HEIGHT)
	if err != nil {
		log.Printf("❌ HTTP serveFrame: Failed to copy middle frame (172×266 at y=%d): %v", PCAT2_TOP_BAR_HEIGHT, err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to copy middle frame: " + err.Error())
	}

	// Footer: 172×22 at y=298
	err = copyImageToImageAt(webFrame, footerBuffer, 0, PCAT2_LCD_HEIGHT-PCAT2_FOOTER_HEIGHT)
	if err != nil {
		log.Printf("❌ HTTP serveFrame: Failed to copy footer frame (172×22 at y=%d): %v", PCAT2_LCD_HEIGHT-PCAT2_FOOTER_HEIGHT, err)
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to copy footer frame: " + err.Error())
	}
	frameMutex.RUnlock()

	if webFrame == nil {
		return c.Status(fiber.StatusServiceUnavailable).SendString("No frame available")
	}

	err = png.Encode(&buf, webFrame)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).SendString("Failed to encode image")
	}

	c.Set("Content-Type", "image/png")
	c.Set("Content-Length", strconv.Itoa(buf.Len()))
	return c.Send(buf.Bytes())
}

// simple index
func indexHandler(c *fiber.Ctx) error {
	return c.SendFile("assets/html/index.html")
}

// GET  /api/v1/changePage
func changePage(c *fiber.Ctx) error {
	lastActivityMu.Lock()
	httpChangePageTriggered = true
	lastActivity = time.Now() // Set to current time to avoid triggering fade-in
	lastActivityMu.Unlock()
	
	// Signal the main loop to interrupt FPS sleep
	signalPageChange()
	
	// Set swippingScreen to prevent backlight fade-in during HTTP page changes
	swippingScreen = true
	
	// Invalidate pre-calculated data since page is changing via HTTP
	// invalidatePreCalculatedData() // Function temporarily disabled
	
	return c.JSON(fiber.Map{"status": "page change triggered"})
}

// GET  /api/v1/data.json
func getData(c *fiber.Ctx) error {
	// 1) Build a plain map from the sync.Map
	out := make(map[string]interface{})

	globalData.Range(func(key, value interface{}) bool {
		// assume your keys are strings
		if ks, ok := key.(string); ok {
			out[ks] = value
		}
		return true // continue iteration
	})

	// 2) Return that map as JSON
	return c.JSON(out)
}

// loadUserConfig reads existing file into globalData
func loadUserConfig() string {
	if userJsonConfig != "" {
		return userJsonConfig
	}
	path := ETC_USER_CONFIG_PATH
	raw, err := os.ReadFile(path)

	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("no existing user config at %s, starting fresh", path)
		} else {
			log.Printf("error reading user config: %v", err)
		}
		userJsonConfig = "{}"
		userCfg = Config{}
		return "{}"
	}

	var m map[string]string
	if err := secureUnmarshal(raw, &m); err != nil {
		log.Printf("error parsing user config JSON: %v", err)
		userJsonConfig = "{}"
		userCfg = Config{}
		return "{}"
	}

	for k, v := range m {
		globalData.Store(k, v)
	}
	log.Printf("loaded %d entries from user config", len(m))
	userJsonConfig = string(raw)
	return userJsonConfig
}

// saveUserConfigToFile writes the userCfg struct to ETC_USER_CONFIG_PATH atomically.
// Returns true on success, false on any error.
func saveUserConfigToFile() bool {
	// 1) Marshal with indentation
	data, err := json.MarshalIndent(userCfg, "", "  ")
	if err != nil {
		log.Printf("could not marshal user config: %v", err)
		return false
	}

	// 2) Ensure directory exists
	if err := os.MkdirAll(filepath.Dir(ETC_USER_CONFIG_PATH), 0755); err != nil {
		log.Printf("could not create config dir: %v", err)
		return false
	}

	// 3) Write to temp file
	tmpPath := ETC_USER_CONFIG_PATH + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		log.Printf("could not write temp user config: %v", err)
		return false
	}

	// 4) Rename temp file into place
	if err := os.Rename(tmpPath, ETC_USER_CONFIG_PATH); err != nil {
		log.Printf("could not rename temp config file: %v", err)
		return false
	}

	return true
}

// saveUserConfigFromStr validates the JSON string, pretty‑prints it,
// updates userCfg, and saves it. Returns true on success, false on any error.
func saveUserConfigFromStr(str string) bool {
	// 1) Validate & parse JSON into generic interface
	var obj interface{}
	if err := secureUnmarshal([]byte(str), &obj); err != nil {
		log.Printf("invalid JSON, not saving: %v", err)
		return false
	}

	// 2) Pretty‑print with 4‑space indent
	pretty, err := json.MarshalIndent(obj, "", "    ")
	if err != nil {
		log.Printf("could not marshal indent JSON: %v", err)
		return false
	}

	// 3) Write the prettified JSON to disk atomically
	//    (we skip updating userCfg here since you may not have a struct to unmarshal into;
	//     if you do, unmarshal into it before step 1 and assign to userCfg)
	tmpPath := ETC_USER_CONFIG_PATH + ".tmp"
	if err := os.MkdirAll(filepath.Dir(ETC_USER_CONFIG_PATH), 0755); err != nil {
		log.Printf("could not create config dir: %v", err)
		return false
	}
	if err := os.WriteFile(tmpPath, pretty, 0644); err != nil {
		log.Printf("could not write temp config: %v", err)
		return false
	}
	if err := os.Rename(tmpPath, ETC_USER_CONFIG_PATH); err != nil {
		log.Printf("could not rename temp config into place: %v", err)
		return false
	}

	return true
}

// POST /api/v1/data
func updateData(c *fiber.Ctx) error {
	// 1. Parse the JSON body into a map[string]string
	var payload map[string]string
	if err := c.BodyParser(&payload); err != nil {
		return c.
			Status(fiber.StatusBadRequest).
			JSON(fiber.Map{"error": "invalid JSON"})
	}

	// 2. Store each entry into the sync.Map
	for k, v := range payload {
		globalData.Store(k, v)
	}

	// 3. Return a success response
	return c.JSON(fiber.Map{"status": "ok"})
}

func getDefaultConfig(c *fiber.Ctx) error {
	return c.JSON(cfg)
}

// GET /api/v1/get_user_config.json
func getUserConfig(c *fiber.Ctx) error {
	// 1) Read the file
	data, err := os.ReadFile(ETC_USER_CONFIG_PATH)
	if err != nil {
		log.Printf("could not read user config: %v", err)
		return c.
			Status(fiber.StatusInternalServerError).
			JSON(fiber.Map{
				"status":  "error",
				"message": "unable to load user config",
			})
	}

	// 2) Return raw JSON, set content-type
	c.Type("application/json", "utf-8")
	return c.Send(data)
}

// saveUserConfigFromWeb handles a JSON payload, validates & saves it,
// and returns appropriate HTTP statuses.
func saveUserConfigFromWeb(c *fiber.Ctx) error {
	body := string(c.Body())

	// Attempt to save; this returns false on any validation/write error.
	if ok := saveUserConfigFromStr(body); !ok {
		// Distinguish “invalid JSON” vs “disk/write error”?
		// If you need that, change saveUserConfigFromStr to return (bool, error).
		// For now, we’ll lump all failures under 400 Bad Request.
		return c.Status(fiber.StatusBadRequest).
			JSON(fiber.Map{"status": "error", "message": "invalid JSON or unable to save config"})
	}

	// Success
	//loadAllConfigsToVariables() //TODO: this is probably not needed, but it's a good idea to reload the configs
	return c.JSON(fiber.Map{"status": "ok"})
}

// POST /api/v1/set_user_config.json
func setUserConfig(c *fiber.Ctx) error {
	// 1) Parse incoming JSON into a generic map
	var payload map[string]interface{}
	if err := c.BodyParser(&payload); err != nil {
		return c.
			Status(fiber.StatusBadRequest).
			JSON(fiber.Map{"error": "invalid JSON"})
	}

	// 2) Merge into the in-memory overrides under lock
	configMutex.Lock()
	userOverrides = deepMerge(userOverrides, payload)
	configMutex.Unlock()

	// 3) Persist back to disk
	raw, err := json.MarshalIndent(userOverrides, "", "  ")
	if err != nil {
		log.Printf("warning: could not marshal user_config.json: %v", err)
		return c.
			Status(fiber.StatusInternalServerError).
			JSON(fiber.Map{"error": "could not save config"})
	}
	if err := os.WriteFile("config/user_config.json", raw, 0644); err != nil {
		log.Printf("warning: could not write user_config.json: %v", err)
		return c.
			Status(fiber.StatusInternalServerError).
			JSON(fiber.Map{"error": "could not save config"})
	}

	// 4) Rebuild merged cfg
	mergeConfigs()

	return c.JSON(fiber.Map{"status": "ok"})
}

// GET /api/v1/get_config.json
func getConfig(c *fiber.Ctx) error {
	configMutex.RLock()
	defer configMutex.RUnlock()
	return c.JSON(cfg)
}

// POST /api/v1/set_config.json
func setConfig(c *fiber.Ctx) error {
	// Parse incoming JSON as generic map
	var payload map[string]interface{}
	if err := c.BodyParser(&payload); err != nil {
		return c.Status(fiber.StatusBadRequest).
			JSON(fiber.Map{"error": "invalid JSON"})
	}

	configMutex.Lock()
	defer configMutex.Unlock()

	// Merge new values into overrides
	userOverrides = deepMerge(userOverrides, payload)

	// Persist userOverrides back to disk
	if raw, err := json.MarshalIndent(userOverrides, "", "  "); err != nil {
		log.Printf("warning: could not marshal user_config.json: %v", err)
	} else if err := os.WriteFile("config/user_config.json", raw, 0644); err != nil {
		log.Printf("warning: could not write user_config.json: %v", err)
	}

	// Rebuild merged cfg
	mergeConfigs()

	return c.JSON(fiber.Map{"status": "ok"})
}

// deepMerge merges src into dest (in-place) for nested maps
// deepMerge merges src into dest (in-place) for nested maps, initializing dest if nil
func deepMerge(dest, src map[string]interface{}) map[string]interface{} {
	if dest == nil {
		dest = make(map[string]interface{}, len(src))
	}
	for k, v := range src {
		if vMap, ok := v.(map[string]interface{}); ok {
			// merge nested map
			nested, found := dest[k].(map[string]interface{})
			if !found || nested == nil {
				nested = make(map[string]interface{}, len(vMap))
			}
			dest[k] = deepMerge(nested, vMap)
		} else {
			// override primitive or slice
			dest[k] = v
		}
	}
	return dest
}

// deepCopy returns a deep copy of a map[string]interface{}
func deepCopy(src map[string]interface{}) map[string]interface{} {
	copy := make(map[string]interface{}, len(src))
	for k, v := range src {
		if m, ok := v.(map[string]interface{}); ok {
			copy[k] = deepCopy(m)
		} else {
			copy[k] = v
		}
	}
	return copy
}

func getStatus(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

func resetConfig(c *fiber.Ctx) error {
	cfg = dftCfg
	userCfg = Config{}
	saveUserConfigToFile()
	return c.JSON(fiber.Map{"status": "ok"})
}

// hsvToRgb converts h∈[0,1], s∈[0,1], v∈[0,1] to r,g,b∈[0,1].
func hsvToRgb(h, s, v float64) (r, g, b float64) {
	i := int(h * 6)
	f := h*6 - float64(i)
	p := v * (1 - s)
	q := v * (1 - f*s)
	t := v * (1 - (1-f)*s)
	switch i % 6 {
	case 0:
		r, g, b = v, t, p
	case 1:
		r, g, b = q, v, p
	case 2:
		r, g, b = p, v, t
	case 3:
		r, g, b = p, q, v
	case 4:
		r, g, b = t, p, v
	case 5:
		r, g, b = v, p, q
	}
	return
}

func httpDrawText(c *fiber.Ctx) error {
	// Acquire lock (blocks if another request is drawing)
	now := time.Now().Format("2025-01-01 15:04:05")
	drawMu.Lock()
	defer drawMu.Unlock()

	// 1) Grab the "text" query (empty if missing)
	text := c.Query("text", "")

	// 2) Load your tiny font
	faceTiny, _, err := getFontFace("tiny")
	if err != nil {
		log.Fatalf("Failed to load font: %v", err)
	}

	faceHuge, _, err := getFontFace("huge")
	if err != nil {
		log.Fatalf("Failed to load font: %v", err)
	}

	// 3) Pause your main‐loop so it won’t overwrite
	runMainLoop = false

	// 4) Prepare a blank frame using pool to reduce allocations
	width, height := 172, 320
	frame := GetFrameBuffer(width, height)
	defer ReturnFrameBuffer(frame)

	if text != "" {
		// Draw the provided text centered
		drawText(frame, text, width/2, height/2, faceHuge, PCAT_WHITE, true)
	} else {
		// Seed randomness for a fresh pattern
		rand.Seed(time.Now().UnixNano())
		hueOffset := rand.Float64()
		phase := rand.Float64() * 2 * math.Pi
		freq := 0.01 + rand.Float64()*0.09

		cx, cy := float64(width)/2, float64(height)/2
		for x := 0; x < width; x++ {
			for y := 0; y < height; y++ {
				dx, dy := float64(x)-cx, float64(y)-cy
				angle := math.Atan2(dy, dx)
				hue := math.Mod((angle+math.Pi)/(2*math.Pi)+hueOffset, 1.0)
				dist := math.Hypot(dx, dy)
				val := 0.5 + 0.5*math.Sin(dist*freq+phase)

				rF, gF, bF := hsvToRgb(hue, 1.0, val)
				frame.Set(x, y, color.RGBA{
					R: uint8(rF * 255),
					G: uint8(gF * 255),
					B: uint8(bF * 255),
					A: 255,
				})
			}
		}

		// Overlay current time

		drawText(frame, "no text provided", width/2, height/2-20, faceTiny, PCAT_WHITE, true)
		drawText(frame, now, width/2, height/2, faceTiny, PCAT_WHITE, true)
	}

	// 5) Push to display
	time.Sleep(50 * time.Millisecond) //wait other goroutine to finish, TODO use mutex
	sendFull(display, frame)

	// 6) JSON response
	return c.JSON(fiber.Map{
		"status": "ok",
		"text":   text,
		"time":   now,
	})
}

func makeItRun(c *fiber.Ctx) error {
	weAreRunning = true
	runMainLoop = true
	return c.JSON(fiber.Map{"status": "ok"})
}

func setPingSites(c *fiber.Ctx) error {
	// update in-memory
	configMutex.Lock()
	userCfg.PingSite0 = c.FormValue("ping_site0")
	userCfg.PingSite1 = c.FormValue("ping_site1")
	saveUserConfigToFile() // persist
	configMutex.Unlock()

	return c.JSON(fiber.Map{"status": "ok"})
}

func setScreenDimmerTime(c *fiber.Ctx) error {
	onBatterySeconds := c.FormValue("screen_dimmer_time_on_battery_seconds")
	onDCSeconds := c.FormValue("screen_dimmer_time_on_dc_seconds")

	onBatterySecondsInt, err := strconv.Atoi(onBatterySeconds)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"status": "error", "message": "Invalid screen_dimmer_time_on_battery_seconds"})
	}

	onDCSecondsInt, err := strconv.Atoi(onDCSeconds)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"status": "error", "message": "Invalid screen_dimmer_time_on_dc_seconds"})
	}

	configMutex.Lock()
	userCfg.ScreenDimmerTimeOnBatterySeconds = onBatterySecondsInt
	userCfg.ScreenDimmerTimeOnDCSeconds = onDCSecondsInt
	saveUserConfigToFile()
	configMutex.Unlock()

	return c.JSON(fiber.Map{"status": "ok"})
}

func setShowSMS(c *fiber.Ctx) error {
	raw := c.FormValue("showSMS")

	valid_values := []string{"true", "false"}

	if !slices.Contains(valid_values, raw) {
		return c.Status(400).JSON(fiber.Map{
			"status":  "error",
			"message": "showSMS must be boolean",
		})
	}

	configMutex.Lock()
	userCfg.ShowSms = (strings.ToLower(raw) == "true")
	saveUserConfigToFile()
	configMutex.Unlock()

	return c.JSON(fiber.Map{"status": "ok", "showSMS": raw})
}

func getMaxBacklight(c *fiber.Ctx) error {
	// Check for runtime override first
	runtimeBrightnessMu.RLock()
	if runtimeMaxBrightness != nil {
		brightness := *runtimeMaxBrightness
		runtimeBrightnessMu.RUnlock()
		return c.JSON(fiber.Map{
			"status": "ok",
			"max_brightness": brightness,
			"source": "runtime_override",
		})
	}
	runtimeBrightnessMu.RUnlock()

	// Use config.json value
	configMutex.RLock()
	maxBrightness := cfg.ScreenMaxBrightness
	configMutex.RUnlock()

	return c.JSON(fiber.Map{
		"status": "ok",
		"max_brightness": maxBrightness,
		"source": "config",
	})
}

func setMaxBacklight(c *fiber.Ctx) error {
	raw := c.FormValue("max_brightness")
	if raw == "" {
		return c.Status(400).JSON(fiber.Map{
			"status":  "error",
			"message": "max_brightness parameter is required",
		})
	}

	maxBrightness, err := strconv.Atoi(raw)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{
			"status":  "error",
			"message": "max_brightness must be a valid integer",
		})
	}

	if maxBrightness < 0 || maxBrightness > 100 {
		return c.Status(400).JSON(fiber.Map{
			"status":  "error",
			"message": "max_brightness must be between 0 and 100",
		})
	}

	// Set runtime override instead of modifying config
	runtimeBrightnessMu.Lock()
	runtimeMaxBrightness = &maxBrightness
	runtimeBrightnessMu.Unlock()

	return c.JSON(fiber.Map{
		"status": "ok",
		"max_brightness": maxBrightness,
		"source": "runtime_override",
		"message": "Runtime brightness override set (does not modify config.json)",
	})
}

func resetMaxBacklight(c *fiber.Ctx) error {
	// Clear runtime override to return to config.json values
	runtimeBrightnessMu.Lock()
	runtimeMaxBrightness = nil
	runtimeBrightnessMu.Unlock()

	// Get current config value to return
	configMutex.RLock()
	maxBrightness := cfg.ScreenMaxBrightness
	configMutex.RUnlock()

	return c.JSON(fiber.Map{
		"status": "ok",
		"max_brightness": maxBrightness,
		"source": "config",
		"message": "Runtime brightness override cleared, now following config.json",
	})
}

// getEffectiveMaxBrightness returns the runtime override if set, otherwise config value
func getEffectiveMaxBrightness() int {
	runtimeBrightnessMu.RLock()
	if runtimeMaxBrightness != nil {
		brightness := *runtimeMaxBrightness
		runtimeBrightnessMu.RUnlock()
		return brightness
	}
	runtimeBrightnessMu.RUnlock()

	// Return config value
	configMutex.RLock()
	maxBrightness := cfg.ScreenMaxBrightness
	configMutex.RUnlock()
	return maxBrightness
}

func httpServer(port string) {
	app := fiber.New()

	// Routes
	app.Get("/", indexHandler)
	app.Get("/api/v1/go_frame.png", serveFrame)
	app.Get("/api/v1/go_data.json", getData)     //TODO: add content
	app.Post("/api/v1/go_data.json", updateData) //TODO: add content
	app.Get("/api/v1/go_changePage", changePage)
	app.Get("/api/v1/go_display_text.json", httpDrawText)
	app.Get("/api/v1/go_make_it_run", makeItRun)
	//get/set configs (json)
	app.Get("/api/v1/go_get_default_config.json", getDefaultConfig)
	app.Get("/api/v1/go_get_config.json", getConfig)
	app.Get("/api/v1/go_get_user_config.json", getUserConfig)
	app.Post("/api/v1/go_save_user_config.json", saveUserConfigFromWeb)
	app.Post("/api/v1/go_set_user_config.json", setUserConfig)
	app.Get("/api/v1/go_get_status.json", getStatus)
	app.Get("/api/v1/go_reset_config", resetConfig)

	//get/set individual configs
	app.Post("/api/v1/go_set_ping_sites", setPingSites)
	app.Post("/api/v1/go_set_screen_dimmer_time", setScreenDimmerTime)
	app.Post("/api/v1/go_set_show_sms", setShowSMS)
	app.Get("/api/v1/go_get_max_backlight", getMaxBacklight)
	app.Post("/api/v1/go_set_max_backlight", setMaxBacklight)
	app.Post("/api/v1/go_reset_max_backlight", resetMaxBacklight)

	// Custom metrics endpoints
	app.Get("/api/v1/custom_metrics/status", getCustomMetricsStatus)
	app.Post("/api/v1/custom_metrics/data", updateCustomMetricsData)
	app.Post("/api/v1/custom_metrics/:source/execute", executeCustomMetricsSource)

	// Start server, retry if failed
	var ln net.Listener
	var err error
	for {
		ln, err = net.Listen("tcp", port)
		if err != nil {
			log.Printf("cannot bind to %s: %v — retrying in 2s…", port, err)
			time.Sleep(2 * time.Second)
			continue
		}
		break
	}

	log.Println("Successfully bound to", port)
	log.Fatal(app.Listener(ln))
}

// ============================================================================
// Custom Metrics Handlers
// ============================================================================

// getCustomMetricsStatus returns the status of all custom metric sources
func getCustomMetricsStatus(c *fiber.Ctx) error {
	if customMetricsMgr == nil {
		return c.Status(404).JSON(fiber.Map{
			"error": "custom metrics manager not initialized",
		})
	}

	statuses := customMetricsMgr.GetAllStatus()
	return c.JSON(fiber.Map{
		"sources": statuses,
		"count":   len(statuses),
	})
}

// updateCustomMetricsData updates custom metrics data (for HTTP sources)
func updateCustomMetricsData(c *fiber.Ctx) error {
	if customMetricsMgr == nil {
		return c.Status(404).JSON(fiber.Map{
			"error": "custom metrics manager not initialized",
		})
	}

	var data map[string]interface{}
	if err := c.BodyParser(&data); err != nil {
		return c.Status(400).JSON(fiber.Map{
			"error": "invalid JSON",
		})
	}

	// Find HTTP sources and update them
	updated := false
	for _, source := range customMetricsMgr.sources {
		if httpSource, ok := source.(*HTTPSource); ok {
			if err := httpSource.UpdateData(data); err != nil {
				return c.Status(500).JSON(fiber.Map{
					"error": err.Error(),
				})
			}
			updated = true
		}
	}

	if !updated {
		// No HTTP source found, just store the data directly
		for key, value := range data {
			globalData.Store(key, fmt.Sprint(value))
		}
	}

	return c.JSON(fiber.Map{
		"status":  "ok",
		"updated": len(data),
	})
}

// executeCustomMetricsSource triggers immediate execution of a command source
func executeCustomMetricsSource(c *fiber.Ctx) error {
	if customMetricsMgr == nil {
		return c.Status(404).JSON(fiber.Map{
			"error": "custom metrics manager not initialized",
		})
	}

	sourceName := c.Params("source")
	if sourceName == "" {
		return c.Status(400).JSON(fiber.Map{
			"error": "source name is required",
		})
	}

	source := customMetricsMgr.GetSourceByName(sourceName)
	if source == nil {
		return c.Status(404).JSON(fiber.Map{
			"error": fmt.Sprintf("source '%s' not found", sourceName),
		})
	}

	// Only command sources support immediate execution
	if cmdSource, ok := source.(*CommandSource); ok {
		cmdSource.ExecuteNow()
		return c.JSON(fiber.Map{
			"status": "triggered",
			"source": sourceName,
		})
	}

	return c.Status(400).JSON(fiber.Map{
		"error": fmt.Sprintf("source '%s' does not support immediate execution", sourceName),
	})
}
