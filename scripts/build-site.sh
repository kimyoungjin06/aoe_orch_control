#!/usr/bin/env bash
set -euo pipefail

# Build the full Forager website
# Output: dist/ directory ready for deployment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
DIST_DIR="$ROOT_DIR/dist"

echo "Building Forager website..."

# Clean previous build
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

# 1. Build mdbook documentation
echo "Building documentation with mdbook..."
if command -v mdbook &> /dev/null; then
    (cd "$ROOT_DIR" && mdbook build)
else
    echo "Error: mdbook not found. Install with: cargo install mdbook"
    exit 1
fi

# 2. Copy shared assets to website/public before Astro build
echo "Copying assets to website..."
mkdir -p "$ROOT_DIR/website/public/assets"
cp "$ROOT_DIR/assets/logo.svg" "$ROOT_DIR/website/public/assets/"
cp "$ROOT_DIR/assets/logo.png" "$ROOT_DIR/website/public/assets/"
cp "$ROOT_DIR/assets/logo-lockup.svg" "$ROOT_DIR/website/public/assets/" 2>/dev/null || true
cp "$ROOT_DIR/assets/kisti-logo-en.png" "$ROOT_DIR/website/public/assets/"
cp "$ROOT_DIR/assets/social-preview.png" "$ROOT_DIR/website/public/assets/" 2>/dev/null || true
cp "$ROOT_DIR/assets/social-preview.svg" "$ROOT_DIR/website/public/assets/" 2>/dev/null || true
cp "$ROOT_DIR/theme/favicon.png" "$ROOT_DIR/website/public/assets/" 2>/dev/null || true
cp "$ROOT_DIR/docs/assets/tui.png" "$ROOT_DIR/website/public/assets/"

# 3. Build Astro website
echo "Building Astro website..."
(cd "$ROOT_DIR/website" && npm ci && npm run build)

# 4. Copy Astro output to dist/
echo "Copying website..."
cp -r "$ROOT_DIR/website/dist/"* "$DIST_DIR/"

# 5. Copy mdbook output to dist/docs/
echo "Copying documentation..."
cp -r "$ROOT_DIR/book" "$DIST_DIR/docs"

# 6. Copy install script
echo "Copying install script..."
cp "$ROOT_DIR/scripts/install.sh" "$DIST_DIR/"

# 7. Create a simple 404 page that redirects to home
cat > "$DIST_DIR/404.html" << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Page Not Found - Forager</title>
  <link rel="icon" type="image/svg+xml" href="/forager-cli/assets/logo.svg">
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-950 text-gray-100 min-h-screen flex items-center justify-center">
  <div class="text-center px-6">
    <img src="/forager-cli/assets/logo.svg" alt="Forager" class="w-16 h-16 mx-auto mb-6 opacity-50">
    <h1 class="text-4xl font-bold mb-4">404</h1>
    <p class="text-gray-400 mb-8">Page not found</p>
    <a href="/forager-cli/" class="bg-sky-700 hover:bg-sky-600 text-white font-semibold px-6 py-3 rounded-lg transition-colors">
      Go Home
    </a>
  </div>
</body>
</html>
EOF

echo ""
echo "Build complete! Output in: $DIST_DIR"
echo ""
echo "Directory structure:"
find "$DIST_DIR" -type f -print 2>/dev/null | head -20 | sed "s|$DIST_DIR|dist|" || true
echo ""
echo "To preview locally:"
echo "  mkdir -p $ROOT_DIR/target/site-preview"
echo "  ln -sfn $DIST_DIR $ROOT_DIR/target/site-preview/forager-cli"
echo "  cd $ROOT_DIR/target/site-preview && python3 -m http.server 8000"
echo "  open http://127.0.0.1:8000/forager-cli/"
echo ""
echo "To deploy to GitHub Pages, Cloudflare Pages, or Netlify:"
echo "  Point your deployment to the dist/ directory"
