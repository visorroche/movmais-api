import { Router } from 'express';
import type { Request, Response } from 'express';
import fs from 'node:fs';
import path from 'node:path';
import { usersRouter } from './api/users.routes';
import { companiesRouter } from './api/companies.routes';
import { groupsRouter } from './api/groups.routes';
import { platformsRouter } from './api/platforms.routes';

export const router = Router();

router.get('/health', (req: Request, res: Response) => {
  res.json({ ok: true });
});

function readVersionFromApiProject(): { version: string | null; file: string | null } {
  const candidates = [
    // Queremos especificamente o arquivo dentro do projeto API: ./api/version.txt
    //
    // 1) quando o processo roda dentro de ./api
    path.resolve(process.cwd(), 'version.txt'),
    // 2) quando o processo roda na raiz do repo
    path.resolve(process.cwd(), 'api', 'version.txt'),
    // 3) quando roda a partir de ./api/dist (produção)
    path.resolve(__dirname, '..', 'version.txt'),
    // 4) quando roda a partir de ./api/src (dev)
    path.resolve(__dirname, '..', '..', 'version.txt'),
  ];

  for (const p of candidates) {
    try {
      if (!fs.existsSync(p)) continue;
      const raw = fs.readFileSync(p, 'utf8');
      const version = String(raw).trim();
      return { version: version || null, file: p };
    } catch {
      // tenta próximo candidato
    }
  }
  return { version: null, file: null };
}

router.get('/version', (req: Request, res: Response) => {
  const { version, file } = readVersionFromApiProject();
  if (!version) {
    return res
      .status(404)
      .json({ version: null, error: 'api/version.txt não encontrado ou vazio', file });
  }
  return res.json({ version });
});

router.use('/users', usersRouter);
router.use('/companies', companiesRouter);
router.use('/groups', groupsRouter);
router.use('/platforms', platformsRouter);
