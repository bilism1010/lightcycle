import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
export default defineConfig({
  root: here('.'),
  base: './',
  plugins: [react()],
  resolve: {
    alias: [
      { find: /^lumagrid\/theme$/, replacement: here('../src/theme/index.ts') },
      { find: /^lumagrid\/viz$/, replacement: here('../src/viz/index.ts') },
      { find: /^lumagrid$/, replacement: here('../src/index.ts') },
    ],
  },
  build: { outDir: here('../../perf-site'), emptyOutDir: true },
});
