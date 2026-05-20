import { defineConfig, loadEnv, type PluginOption } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'path'
import url from 'url'

function vercelApiDev(): PluginOption {
  return {
    name: 'vercel-api-dev',
    configureServer(server) {
      server.middlewares.use(async (req, res, next) => {
        if (!req.url?.startsWith('/api/')) return next()
        const parsed = url.parse(req.url, true)
        const route = parsed.pathname!.replace(/^\/api\//, '').replace(/\/$/, '')
        if (!route) return next()

        const candidates = [
          `/api/${route}.ts`,
          `/api/${route}/index.ts`,
        ]

        type Handler = (req: unknown, res: unknown) => unknown
        let handler: Handler | null = null
        for (const c of candidates) {
          try {
            const m = await server.ssrLoadModule(c) as { default?: Handler }
            if (m?.default) { handler = m.default; break }
          } catch { /* try next */ }
        }
        if (!handler) return next()

        // Body parse for POST/PUT
        let body: unknown = undefined
        if (req.method && ['POST', 'PUT', 'PATCH'].includes(req.method)) {
          const chunks: Buffer[] = []
          for await (const c of req) chunks.push(c as Buffer)
          const raw = Buffer.concat(chunks).toString('utf8')
          try { body = raw ? JSON.parse(raw) : undefined } catch { body = raw }
        }

        Object.assign(req, { query: parsed.query, body })
        const resShim: typeof res & { status: (c: number) => unknown; json: (o: unknown) => unknown; send: (d: unknown) => unknown } = Object.assign(res, {
          status(code: number) { res.statusCode = code; return resShim },
          json(obj: unknown) {
            res.setHeader('Content-Type', 'application/json')
            res.end(JSON.stringify(obj))
            return resShim
          },
          send(data: unknown) {
            if (typeof data === 'object') {
              res.setHeader('Content-Type', 'application/json')
              res.end(JSON.stringify(data))
            } else {
              res.end(String(data))
            }
            return resShim
          },
        })

        try {
          await handler(req, resShim)
        } catch (e) {
          console.error(`[api] ${route} error:`, e)
          if (!res.headersSent) {
            res.statusCode = 500
            res.setHeader('Content-Type', 'application/json')
            res.end(JSON.stringify({ error: String(e) }))
          }
        }
      })
    },
  }
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, path.resolve(__dirname, '..'), '')
  if (env.DATABASE_URL) process.env.DATABASE_URL = env.DATABASE_URL

  return {
    envDir: path.resolve(__dirname, '..'),
    plugins: [react(), vercelApiDev()],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
  }
})
