import express, { type Response } from 'express';
import { createServer } from 'http';
import { WebSocketServer } from 'ws';
import path from 'path';
import { fileURLToPath } from 'url';
import { apiRoutes } from './routes.js';
import { agentRoutes } from './agent-routes.js';
import { setupWebSocket } from './ws.js';
import { createBridgeMountRouter } from './bridge.js';
import { getCollabRuntime, startCollabRuntimeEmbedded } from './collab.js';
import { discoveryRoutes } from './discovery-routes.js';
import { shareWebRoutes } from './share-web-routes.js';
import {
  capabilitiesPayload,
  enforceApiClientCompatibility,
  enforceBridgeClientCompatibility,
} from './client-capabilities.js';
import { getBuildInfo } from './build-info.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const PORT = Number.parseInt(process.env.PORT || '4000', 10);
const DEFAULT_ALLOWED_CORS_ORIGINS = [
  'http://localhost:3000',
  'http://127.0.0.1:3000',
  'http://localhost:4000',
  'http://127.0.0.1:4000',
  'null',
];

function readExperimentalLabel(): string | null {
  const raw = (process.env.PROOF_EXPERIMENTAL_LABEL || '').trim();
  return raw.length > 0 ? raw : null;
}

function parseAllowedCorsOrigins(): Set<string> {
  const configured = (process.env.PROOF_CORS_ALLOW_ORIGINS || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  return new Set(configured.length > 0 ? configured : DEFAULT_ALLOWED_CORS_ORIGINS);
}

async function main(): Promise<void> {
  const app = express();
  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  const allowedCorsOrigins = parseAllowedCorsOrigins();
  const shouldDisableStaticCaching = process.env.NODE_ENV !== 'production';
  const staticAssetOptions = shouldDisableStaticCaching
    ? {
      index: false,
      setHeaders: (res: Response) => {
        res.setHeader('Cache-Control', 'no-store, max-age=0');
        res.setHeader('Pragma', 'no-cache');
        res.setHeader('Expires', '0');
      },
    }
    : { index: false };

  app.use(express.json({ limit: '10mb' }));
  app.use(express.static(path.join(__dirname, '..', 'public'), staticAssetOptions));
  app.use(express.static(path.join(__dirname, '..', 'dist'), staticAssetOptions));

  app.use((req, res, next) => {
    const originHeader = req.header('origin');
    if (originHeader && allowedCorsOrigins.has(originHeader)) {
      res.setHeader('Access-Control-Allow-Origin', originHeader);
      res.setHeader('Vary', 'Origin');
    }
    res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
    res.setHeader(
      'Access-Control-Allow-Headers',
      [
        'Content-Type',
        'Authorization',
        'X-Proof-Client-Version',
        'X-Proof-Client-Build',
        'X-Proof-Client-Protocol',
        'x-share-token',
        'x-bridge-token',
        'x-auth-poll-token',
        'X-Agent-Id',
        'X-Window-Id',
        'X-Document-Id',
        'Idempotency-Key',
        'X-Idempotency-Key',
      ].join(', '),
    );
    if (req.method === 'OPTIONS') {
      res.status(204).end();
      return;
    }
    next();
  });

  app.get('/', (_req, res) => {
    const experimentalLabel = readExperimentalLabel();
    const experimentalMarkup = experimentalLabel
      ? `<div style="display:inline-flex;align-items:center;gap:8px;margin:0 0 1rem;">
      <span style="display:inline-flex;align-items:center;padding:0.4rem 0.7rem;border-radius:999px;background:#efe7ff;color:#5b21b6;font-size:0.92rem;font-weight:600;">${experimentalLabel}</span>
    </div>`
      : '';
    res.type('html').send(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Proof SDK</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, sans-serif; margin: 0; padding: 48px 24px; color: #17261d; background: #f7faf5; }
      main { max-width: 760px; margin: 0 auto; }
      h1 { font-size: 2.5rem; margin: 0 0 0.5rem; }
      p { font-size: 1.05rem; line-height: 1.6; }
      code { background: #eaf2e6; padding: 0.2rem 0.35rem; border-radius: 4px; }
      a { color: #266854; }
      .actions { display: flex; flex-wrap: wrap; gap: 12px; align-items: center; margin: 24px 0 12px; }
      .primary-btn {
        appearance: none; border: none; border-radius: 999px; background: #17261d; color: #fff;
        font: inherit; font-weight: 600; padding: 0.85rem 1.2rem; cursor: pointer;
        box-shadow: 0 10px 24px rgba(23, 38, 29, 0.14);
      }
      .primary-btn:disabled { opacity: 0.7; cursor: progress; }
      .status { min-height: 1.4em; font-size: 0.95rem; color: #4b5563; }
      .subtle { color: #4b5563; }
    </style>
  </head>
  <body>
    <main>
      ${experimentalMarkup}
      <h1>Proof SDK</h1>
      <p>Open-source collaborative markdown editing with provenance tracking and an agent HTTP bridge.</p>
      <div class="actions">
        <button id="new-document-btn" class="primary-btn" type="button">New document</button>
        <span id="create-status" class="status" role="status" aria-live="polite"></span>
      </div>
      <p class="subtle">The button creates a fresh shared doc and opens it with an editor token.</p>
      <p>Start with <code>POST /documents</code>, inspect <a href="/agent-docs">agent docs</a>, or read <a href="/.well-known/agent.json">discovery metadata</a>.</p>
    </main>
    <script>
      const newDocumentButton = document.getElementById('new-document-btn');
      const createStatus = document.getElementById('create-status');

      const setCreateStatus = (message) => {
        if (createStatus) createStatus.textContent = message;
      };

      if (newDocumentButton instanceof HTMLButtonElement) {
        newDocumentButton.addEventListener('click', async () => {
          if (newDocumentButton.disabled) return;
          newDocumentButton.disabled = true;
          newDocumentButton.textContent = 'Creating...';
          setCreateStatus('Creating a fresh document...');
          try {
            const response = await fetch('/documents', {
              method: 'POST',
              headers: {
                'Content-Type': 'application/json',
                'X-Proof-Client-Version': '0.31.0',
                'X-Proof-Client-Build': 'web-home',
                'X-Proof-Client-Protocol': '3',
              },
              body: JSON.stringify({
                markdown: '# Untitled\\n\\n',
              }),
            });
            const payload = await response.json().catch(() => null);
            if (!response.ok) {
              throw new Error((payload && typeof payload.error === 'string' && payload.error) || 'Document creation failed');
            }
            const destination = payload?.tokenUrl || payload?.shareUrl || payload?.url;
            if (typeof destination !== 'string' || destination.length === 0) {
              throw new Error('Document create route did not return a URL');
            }
            window.location.href = destination;
          } catch (error) {
            const message = error instanceof Error ? error.message : 'Document creation failed';
            setCreateStatus(message);
            newDocumentButton.disabled = false;
            newDocumentButton.textContent = 'New document';
          }
        });
      }
    </script>
  </body>
</html>`);
  });

  app.get('/health', (_req, res) => {
    const buildInfo = getBuildInfo();
    res.json({
      ok: true,
      buildInfo,
      collab: getCollabRuntime(),
    });
  });

  app.get('/api/capabilities', (_req, res) => {
    res.json(capabilitiesPayload());
  });

  app.use(discoveryRoutes);
  app.use('/api', enforceApiClientCompatibility, apiRoutes);
  app.use('/api/agent', agentRoutes);
  app.use(apiRoutes);
  app.use('/d', createBridgeMountRouter(enforceBridgeClientCompatibility));
  app.use('/documents', createBridgeMountRouter(enforceBridgeClientCompatibility));
  app.use('/documents', agentRoutes);
  app.use(shareWebRoutes);

  setupWebSocket(wss);
  await startCollabRuntimeEmbedded(PORT);

  server.listen(PORT, () => {
    console.log(`[proof-sdk] listening on http://127.0.0.1:${PORT}`);
  });
}

main().catch((error) => {
  console.error('[proof-sdk] failed to start server', error);
  process.exit(1);
});
