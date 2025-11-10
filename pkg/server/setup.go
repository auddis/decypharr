package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"

	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// SetupState tracks the current setup wizard state
type SetupState struct {
	Completed      bool   `json:"completed"`
	CurrentStep    int    `json:"current_step"`
	Username       string `json:"username,omitempty"`
	DebridProvider string `json:"debrid_provider,omitempty"`
	DebridAPIKey   string `json:"debrid_api_key,omitempty"`
	MountFolder    string `json:"mount_folder,omitempty"`
	DownloadFolder string `json:"download_folder,omitempty"`
	MountSystem    string `json:"mount_system,omitempty"` // "dfs" or "rclone"
	MountPath      string `json:"mount_path,omitempty"`
	CacheDir       string `json:"cache_dir,omitempty"`
}

// SetupWizardRequest represents a request from the setup wizard
type SetupWizardRequest struct {
	Step int                    `json:"step"`
	Data map[string]interface{} `json:"data"`
}

// SetupWizardResponse represents the response from setup wizard
type SetupWizardResponse struct {
	Success      bool        `json:"success"`
	Message      string      `json:"message,omitempty"`
	Error        string      `json:"error,omitempty"`
	NextStep     int         `json:"next_step,omitempty"`
	State        *SetupState `json:"state,omitempty"`
	Validation   interface{} `json:"validation,omitempty"`
	SetupNeeded  bool        `json:"setup_needed,omitempty"`
	RedirectTo   string      `json:"redirect_to,omitempty"`
	ConfigLoaded bool        `json:"config_loaded,omitempty"`
}

// SetupHandler renders the setup wizard page
func (s *Server) SetupHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase": cfg.URLBase,
		"Page":    "setup",
		"Title":   "Setup Wizard",
	}
	err := s.templates.ExecuteTemplate(w, "setup_layout", data)
	if err != nil {
		s.logger.Error().Err(err).Msg("template error")
	}
}

// sendSetupError sends an error response
func (s *Server) sendSetupError(w http.ResponseWriter, message string, err error) {
	response := SetupWizardResponse{
		Success: false,
		Error:   message,
	}
	if err != nil {
		response.Error = fmt.Sprintf("%s: %v", message, err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(response)
}

// SetupCompleteRequest represents the complete setup data from frontend
type SetupCompleteRequest struct {
	Auth struct {
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"`
		SkipAuth bool   `json:"skip_auth,omitempty"`
	} `json:"auth"`
	Debrid struct {
		Provider string `json:"provider"`
		APIKey   string `json:"api_key"`
	} `json:"debrid"`
	Download struct {
		DownloadFolder string `json:"download_folder"`
	} `json:"download"`
	Mount struct {
		MountType        string `json:"mount_type"`
		MountPath        string `json:"mount_path"`
		CacheDir         string `json:"cache_dir"`
		RcloneBufferSize string `json:"rclone_buffer_size,omitempty"`
	} `json:"mount"`
}

// setupCompleteHandler handles the complete setup in a single request
func (s *Server) setupCompleteHandler(w http.ResponseWriter, r *http.Request) {
	var req SetupCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request format", err)
		return
	}

	cfg := config.Get()

	// Step 1: Handle Authentication
	if req.Auth.SkipAuth {
		cfg.UseAuth = false
	} else if req.Auth.Username != "" && req.Auth.Password != "" {
		auth := cfg.GetAuth()
		if auth == nil {
			auth = &config.Auth{}
		}
		auth.Username = req.Auth.Username

		// Hash password using bcrypt
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Auth.Password), bcrypt.DefaultCost)
		if err != nil {
			s.sendSetupError(w, "Failed to hash password", err)
			return
		}
		auth.Password = string(hashedPassword)

		cfg.UseAuth = true
		if err := cfg.SaveAuth(auth); err != nil {
			s.sendSetupError(w, "Failed to save authentication", err)
			return
		}
	}

	// Step 2: Handle Debrid Account
	if req.Debrid.Provider == "" || req.Debrid.APIKey == "" {
		s.sendSetupError(w, "Debrid provider, API key, and mount folder are required", nil)
		return
	}

	validProviders := map[string]bool{
		"realdebrid": true,
		"alldebrid":  true,
		"debridlink": true,
		"torbox":     true,
	}

	if !validProviders[req.Debrid.Provider] {
		s.sendSetupError(w, "Invalid debrid provider", nil)
		return
	}

	debrid := config.Debrid{
		Provider:         req.Debrid.Provider,
		Name:             req.Debrid.Provider,
		APIKey:           req.Debrid.APIKey,
		DownloadAPIKeys:  []string{req.Debrid.APIKey},
		DownloadUncached: false,
		RateLimit:        config.DefaultRateLimit,
	}

	if len(cfg.Debrids) == 0 {
		cfg.Debrids = []config.Debrid{debrid}
	} else {
		cfg.Debrids[0] = debrid
	}

	// Step 3: Handle Download Folder
	if req.Download.DownloadFolder == "" {
		s.sendSetupError(w, "Download folder is required", nil)
		return
	}

	// Create the folder if it doesn't exist
	if err := os.MkdirAll(req.Download.DownloadFolder, 0755); err != nil {
		s.sendSetupError(w, "Failed to create download folder", err)
		return
	}

	cfg.DownloadFolder = req.Download.DownloadFolder

	// Set Manager defaults if not set
	if len(cfg.Categories) == 0 {
		cfg.Categories = []string{"sonarr", "radarr"}
	}
	if cfg.MaxDownloads == 0 {
		cfg.MaxDownloads = 10
	}
	cfg.Mount.Type = config.MountType(req.Mount.MountType)

	if req.Mount.MountType == "dfs" {
		// Configure DFS
		cfg.Mount.MountPath = req.Mount.MountPath

		// Create cache dir
		if err := os.MkdirAll(req.Mount.CacheDir, 0755); err != nil {
			s.sendSetupError(w, "Failed to create cache directory", err)
			return
		}

		cfg.Mount.DFS.CacheDir = req.Mount.CacheDir

		// Set sensible DFS defaults
		if cfg.Mount.DFS.ChunkSize == "" {
			cfg.Mount.DFS.ChunkSize = "8MB"
		}
		if cfg.Mount.DFS.ReadAheadSize == "" {
			cfg.Mount.DFS.ReadAheadSize = "32MB"
		}
		if cfg.Mount.DFS.CacheExpiry == "" {
			cfg.Mount.DFS.CacheExpiry = "24h"
		}
		if cfg.Mount.DFS.AttrTimeout == "" {
			cfg.Mount.DFS.AttrTimeout = "1m"
		}
		if cfg.Mount.DFS.EntryTimeout == "" {
			cfg.Mount.DFS.EntryTimeout = "1m"
		}

	} else if req.Mount.MountType == "rclone" {
		// Configure Rclone
		cfg.Mount.MountPath = req.Mount.MountPath

		if req.Mount.CacheDir != "" {
			cfg.Mount.Rclone.CacheDir = req.Mount.CacheDir
		}

		// Set sensible Rclone defaults
		if cfg.Mount.Rclone.VfsCacheMode == "" {
			cfg.Mount.Rclone.VfsCacheMode = "full"
		}
		if cfg.Mount.Rclone.VfsReadChunkSize == "" {
			cfg.Mount.Rclone.VfsReadChunkSize = "128M"
		}
		if req.Mount.RcloneBufferSize != "" {
			cfg.Mount.Rclone.BufferSize = req.Mount.RcloneBufferSize
		} else if cfg.Mount.Rclone.BufferSize == "" {
			cfg.Mount.Rclone.BufferSize = "128M"
		}
		if cfg.Mount.Rclone.DirCacheTime == "" {
			cfg.Mount.Rclone.DirCacheTime = "5m"
		}
	}

	// Set setup as completed
	cfg.SetupCompleted = true

	if err := cfg.Save(); err != nil {
		s.sendSetupError(w, "Failed to save configuration", err)
		return
	}

	// Trigger manager restart to apply new config
	go s.Restart()

	response := SetupWizardResponse{
		Success:    true,
		Message:    "Setup completed successfully! Restarting services...",
		RedirectTo: "/",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}

// setupSkipHandler handles skipping the setup wizard
func (s *Server) setupSkipHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Skip bool `json:"skip"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request format", err)
		return
	}

	if !req.Skip {
		s.sendSetupError(w, "Invalid skip setup request", nil)
		return
	}

	cfg := config.Get()
	cfg.SetupCompleted = true
	if err := cfg.Save(); err != nil {
		s.sendSetupError(w, "Failed to save configuration", err)
		return
	}

	response := SetupWizardResponse{
		Success:    true,
		Message:    "Setup skipped successfully",
		RedirectTo: "/",
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(response)
}
