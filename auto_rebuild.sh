#!/bin/bash

# Auto-rebuild macOS app when scraper_gui.py changes
# Development tool only - not needed for end users

echo "🔍 Watching scraper_gui.py for changes..."
echo "   When you save the file, the .app will automatically rebuild"
echo "   Press Ctrl+C to stop watching"
echo ""

# Check if fswatch is installed
if ! command -v fswatch &> /dev/null; then
    echo "⚠️  fswatch not found. Installing via Homebrew..."
    brew install fswatch
fi

# Watch scraper_gui.py and rebuild on changes
fswatch -r scraper_gui.py 2>/dev/null | while read -r line; do
    echo ""
    echo "📝 GUI file changed, rebuilding app..."
    bash build_mac_app.command
    echo "✅ Rebuild complete! Your changes are live in the .app"
    echo ""
    echo "🔍 Watching for more changes..."
done
