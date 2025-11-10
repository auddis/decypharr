// Setup Wizard JavaScript
class SetupWizard {
    constructor() {
        this.currentStep = 1;
        this.setupState = {
            step1: {},
            step2: {},
            step3: {},
            step4: {},
        };

        this.refs = {
            loadingState: document.getElementById('loading-state'),
            errorState: document.getElementById('error-state'),
            errorMessage: document.getElementById('error-message'),
        };

        this.init();
    }

    init() {
        this.initializeWizard();
    }

    async initializeWizard() {
        try {
            // Load existing config to populate fields
            await this.loadExistingConfig();

            this.hideLoading();
            this.showStep(this.currentStep);
            this.setupEventListeners();
        } catch (error) {
            console.error('Initialization error:', error);
            this.showError('Failed to initialize setup wizard: ' + error.message);
            this.hideLoading();
        }
    }

    async loadExistingConfig() {
        try {
            const response = await window.decypharrUtils.fetcher('/api/config');
            if (response.ok) {
                const config = await response.json();
                this.populateFields(config);
            }
        } catch (error) {
            console.error('Failed to load config:', error);
            // Continue without pre-populating
        }
    }

    populateFields(config) {
        // Populate authentication fields
        if (config.auth_username) {
            document.getElementById('auth-username').value = config.auth_username;
        }

        // Populate debrid fields if exists
        if (config.debrids && config.debrids.length > 0) {
            const debrid = config.debrids[0];
            document.getElementById('debrid-provider').value = debrid.provider || '';
            document.getElementById('debrid-api-key').value = debrid.api_key || '';
        }

        // Populate download folder
        if (config.download_folder) {
            document.getElementById('download-folder').value = config.manager.download_folder;
        }

        // Populate mount settings
        if (config.dfs?.enabled) {
            document.getElementById('mount-type-dfs').checked = true;
            document.getElementById('mount-path').value = config.dfs.mount_path || '';
            document.getElementById('cache-dir').value = config.dfs.cache_dir || '';
        } else if (config.rclone?.enabled) {
            document.getElementById('mount-type-rclone').checked = true;
            document.getElementById('mount-path').value = config.rclone.mount_path || '';
            document.getElementById('cache-dir').value = config.rclone.vfs_cache_dir || '';
            if (config.rclone.buffer_size) {
                document.getElementById('rclone-buffer-size').value = config.rclone.buffer_size;
            }
        }
    }

    setupEventListeners() {
        document.getElementById('skip-setup-btn').addEventListener('click', () => this.handleSkipSetup());
        document.getElementById('auth-next-btn').addEventListener('click', () => this.handleAuthNext());
        document.getElementById('skip-auth-btn').addEventListener('click', () => this.handleSkipAuth());
        document.getElementById('debrid-back-btn').addEventListener('click', () => this.goToStep(1));
        document.getElementById('debrid-next-btn').addEventListener('click', () => this.handleDebridNext());
        document.getElementById('download-back-btn').addEventListener('click', () => this.goToStep(2));
        document.getElementById('download-next-btn').addEventListener('click', () => this.handleDownloadNext());
        document.getElementById('mount-back-btn').addEventListener('click', () => this.goToStep(3));
        document.getElementById('mount-next-btn').addEventListener('click', () => this.handleMountNext());
        document.getElementById('mount-type-dfs').addEventListener('change', () => this.toggleMountOptions());
        document.getElementById('mount-type-rclone').addEventListener('change', () => this.toggleMountOptions());
        document.getElementById('mount-type-external').addEventListener('change', () => this.toggleMountOptions());
        document.getElementById('mount-type-none').addEventListener('change', () => this.toggleMountOptions());
        document.getElementById('overview-back-btn').addEventListener('click', () => this.goToStep(4));
        document.getElementById('finish-btn').addEventListener('click', () => this.handleFinish());
    }

    showLoading() {
        this.refs.loadingState.classList.remove('hidden');
        document.querySelectorAll('.setup-step').forEach(step => step.classList.add('hidden'));
    }

    hideLoading() {
        this.refs.loadingState.classList.add('hidden');
    }

    showError(message) {
        this.refs.errorMessage.textContent = message;
        this.refs.errorState.classList.remove('hidden');
    }

    hideError() {
        this.refs.errorState.classList.add('hidden');
    }

    goToStep(step) {
        this.currentStep = step;
        this.showStep(step);
    }

    showStep(step) {
        this.hideError();
        document.querySelectorAll('.setup-step').forEach(s => s.classList.add('hidden'));

        const stepElement = document.getElementById(`step-${step}`);
        if (stepElement) {
            stepElement.classList.remove('hidden');
        }

        this.updateProgressIndicators(step);

        if (step === 5) {
            this.populateOverview();
        }
    }

    updateProgressIndicators(currentStep) {
        for (let i = 1; i <= 5; i++) {
            const indicator = document.getElementById(`step-indicator-${i}`);
            if (i <= currentStep) {
                indicator.classList.add('step-primary');
            } else {
                indicator.classList.remove('step-primary');
            }
        }
    }

    handleAuthNext() {
        const username = document.getElementById('auth-username').value.trim();
        const password = document.getElementById('auth-password').value;
        const confirmPassword = document.getElementById('auth-confirm-password').value;

        if (username || password || confirmPassword) {
            if (!username) {
                this.showError('Username is required');
                return;
            }
            if (!password) {
                this.showError('Password is required');
                return;
            }
            if (password !== confirmPassword) {
                this.showError('Passwords do not match');
                return;
            }
            if (password.length < 6) {
                this.showError('Password must be at least 6 characters');
                return;
            }

            this.setupState.step1 = {
                username: username,
                password: password,
                skip_auth: false,
            };
        } else {
            this.setupState.step1 = { skip_auth: true };
        }

        this.goToStep(2);
    }

    handleSkipAuth() {
        this.setupState.step1 = { skip_auth: true };
        this.goToStep(2);
    }

    async handleSkipSetup() {
        if (confirm('Are you sure you want to skip the setup wizard? Make sure you have a valid config.json file.')) {
            try {
                const response = await window.decypharrUtils.fetcher('/api/setup/skip', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ skip: true }),
                });

                if (!response.ok) {
                    const errorData = await response.json().catch(() => ({ error: 'Failed to skip setup' }));
                    throw new Error(errorData.error || 'Failed to skip setup');
                }

                window.decypharrUtils.createToast('Setup skipped successfully!', 'success');
                setTimeout(() => {
                    window.location.href = window.decypharrUtils.joinURL(window.urlBase, '/');
                }, 1000);

            } catch (error) {
                console.error('Skip setup error:', error);
                this.showError(error.message || 'Failed to skip setup');
            }
        }
    }

    handleDebridNext() {
        const provider = document.getElementById('debrid-provider').value;
        const apiKey = document.getElementById('debrid-api-key').value.trim();

        if (!provider) {
            this.showError('Please select a debrid provider');
            return;
        }
        if (!apiKey) {
            this.showError('API key is required');
            return;
        }

        this.setupState.step2 = {
            provider: provider,
            api_key: apiKey,
        };

        this.goToStep(3);
    }

    handleDownloadNext() {
        const downloadFolder = document.getElementById('download-folder').value.trim();

        if (!downloadFolder) {
            this.showError('Download folder is required');
            return;
        }

        this.setupState.step3 = {
            download_folder: downloadFolder,
        };

        this.goToStep(4);
    }

    toggleMountOptions() {
        const isDFS = document.getElementById('mount-type-dfs').checked;
        const isRclone = document.getElementById('mount-type-rclone').checked;
        const isNone = document.getElementById('mount-type-none').checked;
        const rcloneOptions = document.getElementById('rclone-options');

        if (isDFS || isNone) {
            rcloneOptions.classList.add('hidden');
        } else {
            rcloneOptions.classList.remove('hidden');
        }
    }

    handleMountNext() {
        const mountType = document.querySelector('input[name="mount_type"]:checked').value;
        const mountPath = document.getElementById('mount-path').value.trim();
        const cacheDir = document.getElementById('cache-dir').value.trim();
        const rcloneBufferSize = document.getElementById('rclone-buffer-size').value.trim();

        if (!mountPath && mountType !== 'none') {
            this.showError('Mount path is required');
            return;
        }

        this.setupState.step4 = {
            mount_type: mountType,
            mount_path: mountPath,
            cache_dir: cacheDir,
        };

        if (mountType === 'rclone' && rcloneBufferSize) {
            this.setupState.step4.rclone_buffer_size = rcloneBufferSize;
        }

        this.goToStep(5);
    }

    populateOverview() {
        const authOverview = document.getElementById('overview-auth');
        if (this.setupState.step1 && this.setupState.step1.skip_auth) {
            authOverview.textContent = 'Authentication disabled (skipped)';
        } else if (this.setupState.step1 && this.setupState.step1.username) {
            authOverview.textContent = `Username: ${this.setupState.step1.username}`;
        } else {
            authOverview.textContent = 'Not configured';
        }

        const debridOverview = document.getElementById('overview-debrid');
        if (this.setupState.step2 && this.setupState.step2.provider) {
            debridOverview.innerHTML = `
                <p><strong>Provider:</strong> ${this.setupState.step2.provider}</p>
            `;
        } else {
            debridOverview.textContent = 'Not configured';
        }

        const downloadOverview = document.getElementById('overview-download');
        downloadOverview.textContent = (this.setupState.step3 && this.setupState.step3.download_folder) || 'Not set';

        const mountOverview = document.getElementById('overview-mount');
        if (this.setupState.step4 && this.setupState.step4.mount_type) {
            let mountType = 'None';
            if (this.setupState.step4.mount_type === 'rclone') {
                mountType = 'Rclone';
            } else if (this.setupState.step4.mount_type === 'external_rclone') {
                mountType = 'External Rclone';
            } else if (this.setupState.step4.mount_type === 'dfs') {
                mountType = 'DFS (Decypharr File System)';
            }
            mountOverview.innerHTML = `
                <p><strong>Type:</strong> ${mountType}</p>
                <p><strong>Mount Path:</strong> ${this.setupState.step4.mount_path}</p>
                <p><strong>Cache Directory:</strong> ${this.setupState.step4.cache_dir}</p>
            `;
        } else {
            mountOverview.textContent = 'Not configured';
        }
    }

    async handleFinish() {
        const finishBtn = document.getElementById('finish-btn');
        const finishBtnText = document.getElementById('finish-btn-text');
        const finishBtnLoading = document.getElementById('finish-btn-loading');

        finishBtn.disabled = true;
        finishBtnText.classList.add('hidden');
        finishBtnLoading.classList.remove('hidden');

        try {
            // Collect all data
            const setupData = {
                auth: this.setupState.step1,
                debrid: this.setupState.step2,
                download: this.setupState.step3,
                mount: this.setupState.step4,
            };

            const response = await window.decypharrUtils.fetcher('/api/setup/complete', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(setupData),
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ error: 'Failed to complete setup' }));
                throw new Error(errorData.error || 'Failed to complete setup');
            }

            const data = await response.json();
            window.decypharrUtils.createToast('Setup completed successfully! Redirecting...', 'success');

            setTimeout(() => {
                window.location.href = window.decypharrUtils.joinURL(window.urlBase, '/');
            }, 1500);

        } catch (error) {
            console.error('Finish error:', error);
            this.showError(error.message || 'Failed to complete setup');
            finishBtn.disabled = false;
            finishBtnText.classList.remove('hidden');
            finishBtnLoading.classList.add('hidden');
        }
    }
}
