#!/bin/bash

# Setup development environment for NextSQL
# This script creates a symlink to make the nsql command available system-wide

set -e

# Build release version
echo "Building release version..."
cargo build --release

# Create symlink
NSQL_PATH="$(pwd)/target/release/nsql"
SYMLINK_PATH="$HOME/.local/bin/nsql-dev"

# Create ~/.local/bin if it doesn't exist
mkdir -p "$HOME/.local/bin"

# Create symlink
ln -sf "$NSQL_PATH" "$SYMLINK_PATH"

echo "Created symlink: $SYMLINK_PATH -> $NSQL_PATH"

# Check if ~/.local/bin is in PATH
if ! echo "$PATH" | grep -q "$HOME/.local/bin"; then
    echo ""
    echo "WARNING: $HOME/.local/bin is not in your PATH."
    echo "Add the following line to your shell profile (.bashrc, .zshrc, etc.):"
    echo "export PATH=\"\$HOME/.local/bin:\$PATH\""
    echo ""
fi

# Test the command
echo "Testing nsql-dev command..."
nsql-dev --version 2>/dev/null || nsql-dev --help | head -n 1

echo "Setup complete! You can now use 'nsql-dev' command."