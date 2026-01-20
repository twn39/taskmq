import { defineConfig } from 'vite'
import tailwindcss from '@tailwindcss/vite'

export default defineConfig({
    plugins: [tailwindcss()],
    build: {
        outDir: '../static',
        emptyOutDir: true,
        manifest: true, // Generate manifest.json for asset resolution
        rollupOptions: {
            input: './main.css', // Entry point
        },
    },
})
