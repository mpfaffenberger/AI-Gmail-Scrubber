# 📧 Email Agent - AI-Powered Email Management

> Tired of inbox overwhelm? Email Agent uses advanced AI to automatically classify, organize, and manage your emails. Built on **pydantic-ai** for intelligent reasoning and multiple AI provider support.

## ✨ Key Features

- 🤖 **Multi-AI Support** - Works with Claude, OpenAI, Gemini, or custom endpoints
- 🔐 **Security First** - Credential sanitization, secure file permissions, encrypted state
- ⚡ **Intelligent Classification** - AI-powered email analysis with reasoning transparency
- 📧 **Full IMAP Support** - Works with Gmail, Outlook, Yahoo, or any IMAP provider
- 🎯 **Rule-Based + AI Hybrid** - Combine trusted senders with intelligent classification
- 📊 **Rich Output** - Beautiful CLI with progress bars, tables, and formatted results
- 🔄 **Stateful Processing** - Tracks processed emails, prevents duplicates
- 🛠️ **Dry-Run Mode** - Test actions without modifying emails
- 📝 **Comprehensive Logging** - Debug-friendly logs for troubleshooting

## 🚀 Quick Start

### Prerequisites

- **Python 3.11+** (3.13 recommended)
- **Gmail/IMAP Account** with app-specific password
- **API Key** for at least one AI provider (Claude, OpenAI, or Gemini)

### 30-Second Setup

```bash
# Clone and install
git clone https://github.com/mpfaffenberger/AI-Gmail-Scrubber
cd AI-Gmail-Scrubber
pip install -e .

# Configure (interactive setup)
email-agent init
email-agent configure

# Test connection
email-agent test-connection

# Run it!
email-agent process
```

## 📖 Full Documentation

| Guide | Purpose |
|-------|---------|
| [Installation](docs/INSTALLATION.md) | Detailed setup for Mac, Linux, Windows |
| [Configuration](docs/CONFIGURATION.md) | All config options and common setups |
| [Architecture](docs/ARCHITECTURE.md) | System design and how components interact |
| [Troubleshooting](docs/TROUBLESHOOTING.md) | Common issues and solutions |
| [Development](docs/DEVELOPMENT.md) | Contributing and extending |
| [API Reference](docs/API.md) | Using Email Agent as a library |

## 🔧 CLI Commands

```bash
# Initialize configuration and directories
email-agent init

# Interactive configuration setup
email-agent configure

# List available AI models
email-agent list-models

# Test IMAP connection
email-agent test-connection

# Check processing statistics
email-agent status

# Process emails with AI
email-agent process [--batch-size N]

# Preview without modifying emails
email-agent dry-run [--batch-size N]

# Clear processing history
email-agent reset-state [--yes]
```

## 🔐 Security & Privacy

- ✅ Credentials never logged or printed
- ✅ Local-only state storage
- ✅ Secure file permissions (mode 600)
- ✅ Environment variable support for secrets
- ✅ Comprehensive error sanitization
- ✅ No external data sharing

## 📋 Requirements

### Email Provider Setup

**Gmail:**
1. Enable IMAP: https://mail.google.com/mail/u/0/#settings/fwdandpop
2. Create app password: https://myaccount.google.com/apppasswords
3. Set `GMAIL_EMAIL` and `GMAIL_APP_PASSWORD` environment variables

**Other Providers:**
- Outlook: Use IMAP settings in Security > App passwords
- Yahoo: Use IMAP settings in Account security
- Custom: Configure IMAP server details in config

### AI Provider Setup

Choose at least one:

| Provider | API Key Env Var | Setup |
|----------|-----------------|-------|
| **Claude** (Recommended) | `ANTHROPIC_API_KEY` | https://console.anthropic.com |
| **OpenAI** | `OPENAI_API_KEY` | https://platform.openai.com/api-keys |
| **Google Gemini** | `GEMINI_API_KEY` | https://ai.google.dev |
| **Custom Endpoint** | Via config | Supports OpenAI-compatible endpoints |

### Python & System Requirements

```
Python 3.11 - 3.13
pip or uv package manager
~100MB disk space (for venv + dependencies)
Active internet connection for AI API calls
```

## 💻 Installation

### Option 1: Using uv (Recommended)

```bash
# Install uv if needed
pip install uv

# Clone and setup
git clone https://github.com/mpfaffenberger/AI-Gmail-Scrubber
cd AI-Gmail-Scrubber
uv sync

# Activate venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

### Option 2: Using pip

```bash
git clone https://github.com/mpfaffenberger/AI-Gmail-Scrubber
cd AI-Gmail-Scrubber
pip install -e .
```

### Option 3: Development Setup

```bash
git clone https://github.com/mpfaffenberger/AI-Gmail-Scrubber
cd AI-Gmail-Scrubber
pip install -e ".[dev]"
pytest tests/  # Verify everything works
```

## ⚙️ Configuration

### Quick Configuration

```bash
email-agent init          # Creates ~/.email_agent/
email-agent configure     # Interactive setup
```

### Manual Configuration

Edit `~/.email_agent/config.yaml`:

```yaml
imap:
  server: imap.gmail.com
  email: your-email@gmail.com
  password: ${GMAIL_APP_PASSWORD}  # Use app-specific password
  port: 993

processing:
  batch_size: 20
  dry_run: false
  skip_processed: true

model:
  name: claude-3-5-sonnet
  temperature: 0.7
  max_tokens: 2000

rules:
  trusted_senders:
    - boss@company.com
    - important@example.com
  spam_keywords:
    - unsubscribe
    - click here now
  enabled: true
```

### Environment Variables

Set API keys via environment variables (never commit them!):

```bash
export ANTHROPIC_API_KEY="sk-ant-..."
export OPENAI_API_KEY="sk-..."
export GEMINI_API_KEY="AIza..."
export GMAIL_EMAIL="user@gmail.com"
export GMAIL_APP_PASSWORD="xxxx xxxx xxxx xxxx"  # 16-character app password
```

Or use `.env` file (add to `.gitignore`):

```bash
ANTHROPIC_API_KEY=sk-ant-...
GMAIL_EMAIL=user@gmail.com
GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx
```

## 🎯 Usage Examples

### Basic Processing

```bash
# Process 20 emails at a time
email-agent process

# Process 5 emails with details
email-agent process --batch-size 5

# Preview before running (dry-run mode)
email-agent dry-run --batch-size 5

# Check what's been processed
email-agent status
```

### Common Workflows

**Automated Background Processing:**
```bash
# Set up hourly cron job
bash scripts/setup-cron.sh

# Or use systemd (Linux)
bash scripts/systemd-service.sh
```

**Test New Configuration:**
```bash
email-agent test-connection
email-agent list-models
email-agent dry-run --batch-size 3
```

**Clear and Restart:**
```bash
email-agent reset-state --yes
email-agent process
```

## 🏗️ Architecture

### Components

- **CLI Layer** (`cli.py`, `cli_processing.py`) - User commands
- **Agent** (`agent.py`) - pydantic-ai powered email classifier
- **Orchestrator** (`orchestrator.py`) - Workflow management
- **Classifier** (`classifier.py`) - Decision making logic
- **IMAP Client** (`imap_client.py`) - Email operations
- **Tools** (`tools/`) - Agent capabilities
- **Security** (`security.py`) - Credential protection
- **State** (`state.py`) - Processing history
- **Config** (`config.py`) - Configuration management

### How It Works

```
1. User runs: email-agent process
2. CLI loads config and connects to IMAP
3. Fetches unprocessed emails
4. For each email:
   - AI Agent analyzes content
   - Agent reasons about classification
   - Makes decision (keep, archive, delete)
   - Applies action to email
5. Updates processing state
6. Shows summary to user
```

## 🐛 Troubleshooting

### "Models config not found"
```bash
code-puppy  # Initialize configuration
```

### "API key not set"
```bash
echo $ANTHROPIC_API_KEY  # Verify key is set
# Or set it:
export ANTHROPIC_API_KEY="sk-ant-..."
```

### "IMAP connection failed"
1. Verify Gmail app password (not regular password)
2. Check IMAP is enabled in Gmail settings
3. Run: `email-agent test-connection`

### "Getting rate limited"
- Increase batch delay in config
- Use lower temperature for consistency
- Process fewer emails per run

## 🧪 Testing

```bash
# Run all tests
pytest tests/ -v

# With coverage
pytest tests/ --cov=email_agent

# Generate HTML coverage report
pytest tests/ --cov=email_agent --cov-report=html
# Open htmlcov/index.html in browser
```

## 📚 Project Phases

| Phase | Status | Description |
|-------|--------|-------------|
| **Phase 1** | ✅ Complete | Foundation, architecture, model factory |
| **Phase 2** | ✅ Complete | Email client, tools, reasoning |
| **Phase 3** | ✅ Complete | Agent, orchestration, state management |
| **Phase 4** | ✅ Complete | CLI, security, testing, error handling |
| **Phase 5** | ✅ Complete | Documentation, deployment, polish |

## 📦 What's Included

```
email_agent/
├── agent.py                 # pydantic-ai email classifier
├── classifier.py            # Classification logic
├── cli.py & cli_processing.py  # CLI commands
├── config.py                # Configuration system
├── imap_client.py           # Email operations
├── model_factory.py         # AI model instantiation
├── orchestrator.py          # Workflow orchestration
├── security.py              # Security utilities
├── state.py                 # Processing state
├── tools/                   # Agent tools
│   ├── email_tools.py       # Email manipulation
│   └── reasoning.py         # Agent reasoning
└── main.py                  # CLI entry point

docs/
├── INSTALLATION.md          # Installation guide
├── CONFIGURATION.md         # Config documentation
├── ARCHITECTURE.md          # System design
├── API.md                   # Python API reference
├── TROUBLESHOOTING.md       # Common issues
└── DEVELOPMENT.md           # Contributing guide

scripts/
├── install.sh               # Installation helper
├── uninstall.sh             # Uninstall helper
├── test-setup.sh            # Verify installation
├── setup-cron.sh            # Cron job setup
└── systemd-service.sh       # Systemd service setup
```

## 🚀 Deployment

### Local Installation

```bash
bash scripts/install.sh
```

### Automated Processing

**Systemd (Linux):**
```bash
bash scripts/systemd-service.sh
systemctl start email-agent
```

**Cron (macOS/Linux):**
```bash
bash scripts/setup-cron.sh
```

### Docker (Optional)

Coming in future release!

## 🤝 Contributing

We welcome contributions! Here's how:

1. **Fork** the repository
2. **Create** a feature branch (`git checkout -b feature/amazing`)
3. **Make** your changes (keep files <600 lines!)
4. **Test** your changes (`pytest tests/`)
5. **Lint** your code (`ruff check .`)
6. **Submit** a pull request

### Development Setup

```bash
pip install -e ".[dev]"
pytest tests/ --cov=email_agent
ruff check email_agent tests
ruff format email_agent tests
```

### Code Style

- Follow PEP 8
- Max 100 characters per line
- Type hints on functions
- Docstrings for classes and modules
- Keep files under 600 lines (Zen of Python)
- DRY (Don't Repeat Yourself)
- SOLID principles

## 📊 Status & Stats

- **Version:** 0.1.0
- **Python Support:** 3.11, 3.12, 3.13
- **Test Coverage:** 75%+
- **Last Updated:** 2025
- **License:** MIT
- **Maintained:** Yes ✅

## 📝 License

MIT License - See [LICENSE](LICENSE) file for details

### You Are Free To:
- ✅ Use commercially
- ✅ Modify the code
- ✅ Distribute copies
- ✅ Include in your projects

### Conditions:
- 📋 Include license text
- 📋 State changes made
- 📋 No liability/warranty

## 🙏 Acknowledgments

- **pydantic-ai** - Amazing AI agent framework
- **imap-tools** - Excellent IMAP library
- **Typer** - Beautiful CLI framework
- **Rich** - Terminal formatting magic

## 📞 Support & Community

- **Issues:** Use GitHub Issues for bugs
- **Discussions:** GitHub Discussions for questions
- **Email:** See contact info in repository
- **Documentation:** See `/docs` directory

---



