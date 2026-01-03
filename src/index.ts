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

  res.header('Access-Control-Allow-Headers', process.env.CORS_HEADERS || 'Origin, X-Requested-With, Content-Type, Accept, Authorization');
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
