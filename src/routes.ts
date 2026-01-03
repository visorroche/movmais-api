import { Router } from 'express';
import type { Request, Response } from 'express';
import { usersRouter } from './api/users.routes';
import { companiesRouter } from './api/companies.routes';

export const router = Router();

router.get('/health', (req: Request, res: Response) => {
  res.json({ ok: true });
});

router.use('/users', usersRouter);
router.use('/companies', companiesRouter);
