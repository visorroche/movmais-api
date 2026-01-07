import 'reflect-metadata';
import 'dotenv/config';
import express from 'express';
import type { NextFunction, Request, Response } from 'express';
import { AppDataSource } from './data-source';
import { router } from './routes';
import { setupSwagger } from './swagger';

const app = express();

app.use(express.json({ limit: process.env.JSON_LIMIT || '10mb' }));

app.use((req: Request, res: Response, next: NextFunction) => {
  const origin = req.headers.origin;
  const corsOriginsEnv = process.env.CORS_ORIGINS || '*';
  const allowAll = corsOriginsEnv === '*' || corsOriginsEnv.trim() === '*';

  if (allowAll) {
    res.header('Access-Control-Allow-Origin', origin || '*');
  } else {
    const allowedOrigins = corsOriginsEnv.split(',').map((o: string) => o.trim());
    if (origin && allowedOrigins.includes(origin)) {
      res.header('Access-Control-Allow-Origin', origin);
      res.header('Access-Control-Allow-Credentials', 'true');
    } else if (origin) {
      res.header('Access-Control-Allow-Origin', origin);
    } else {
      res.header('Access-Control-Allow-Origin', '*');
    }
  }

  const requestHeaders = (req.headers['access-control-request-headers'] as string | undefined) || '';
  const baseHeaders =
    process.env.CORS_HEADERS ||
    requestHeaders ||
    'Origin, X-Requested-With, Content-Type, Accept, Authorization';
  const ensureHeader = (headerList: string, headerName: string) => {
    const parts = headerList
      .split(',')
      .map((h) => h.trim())
      .filter(Boolean);
    const has = parts.some((h) => h.toLowerCase() === headerName.toLowerCase());
    return has ? headerList : `${headerList}, ${headerName}`;
  };
  // Garante compatibilidade com o preflight do browser (que manda "x-company-id" em minúsculo)
  let allowHeaders = ensureHeader(baseHeaders, 'Authorization');
  allowHeaders = ensureHeader(allowHeaders, 'X-Company-Id');
  res.header('Access-Control-Allow-Headers', allowHeaders);
  res.header('Access-Control-Allow-Methods', process.env.CORS_METHODS || 'GET, POST, PUT, DELETE, OPTIONS, PATCH');

  if (req.method === 'OPTIONS') {
    return res.sendStatus(204);
  }

  next();
});

setupSwagger(app);

app.use(router);

const PORT = process.env.PORT || 5003;

AppDataSource.initialize()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`Server listening on port ${PORT}`);
      console.log(`[CORS] CORS_ORIGINS: ${process.env.CORS_ORIGINS || '*'}`);
    });
  })
  .catch((err: unknown) => {
    console.error('Error during Data Source initialization', err);
    process.exit(1);
  });
