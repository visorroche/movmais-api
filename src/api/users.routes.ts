import { Router, Request, Response } from 'express';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
 import type { EntityManager } from 'typeorm';
import { AppDataSource } from '../data-source';
import { Users } from '../entities/Users';
import { Companies } from '../entities/Companies';
import { authMiddleware } from '../middleware/auth';

export const usersRouter = Router();

const JWT_SECRET = process.env.JWT_SECRET || 'changeme';

function normalizeRoles(roles: unknown): string[] {
  if (!roles) return [];
  if (Array.isArray(roles)) return roles.filter((r): r is string => typeof r === 'string');
  if (typeof roles === 'string') return [roles];
  if (typeof roles === 'object' && roles !== null) {
    const inner = (roles as any).roles;
    if (Array.isArray(inner)) return inner.filter((r: any): r is string => typeof r === 'string');
    if (typeof inner === 'string') return [inner];
  }
  return [];
}

usersRouter.post('/register', async (req: Request, res: Response) => {
  const { email, password, name, phone, company_name } = req.body as {
    email?: string;
    password?: string;
    name?: string;
    phone?: string;
    company_name?: string;
  };

  if (!email || !password) return res.status(400).json({ message: 'Email and password required' });
  if (!company_name || !String(company_name).trim()) return res.status(400).json({ message: 'Company name required' });

  const normalizedEmail = String(email).trim().toLowerCase();
  if (String(password).length < 8) return res.status(400).json({ message: 'Password must be at least 8 characters' });

  const hash = await bcrypt.hash(String(password), 10);
  const companyName = String(company_name).trim();

  try {
    const result = await AppDataSource.transaction(async (manager: EntityManager) => {
      const usersTx = manager.getRepository(Users);
      const companiesTx = manager.getRepository(Companies);

      const existing = await usersTx.findOne({ where: { email: normalizedEmail } as any });
      if (existing) return { conflict: true as const };

      const company = companiesTx.create({ name: companyName });
      await companiesTx.save(company);

      const user = usersTx.create({
        email: normalizedEmail,
        password: hash,
        name,
        phone,
        roles: [],
        company_id: company.id,
        company,
      });
      await usersTx.save(user);

      company.owner_id = user.id;
      await companiesTx.save(company);

      return { conflict: false as const, user, company };
    });

    if ((result as any).conflict) {
      return res.status(409).json({ message: 'Email already registered' });
    }

    const { user, company } = result as any;
    return res.status(201).json({
      id: user.id,
      email: user.email,
      name: user.name,
      phone: user.phone,
      company_id: user.company_id,
      company: { id: company.id, name: company.name },
    });
  } catch (err: any) {
    if (err?.code === '23505') {
      return res.status(409).json({ message: 'Email already registered' });
    }
    return res.status(500).json({ message: 'Internal server error' });
  }
});

usersRouter.post('/login', async (req: Request, res: Response) => {
  const { email, password } = req.body as { email?: string; password?: string };
  if (!email || !password) return res.status(400).json({ message: 'Email and password required' });

  const normalizedEmail = String(email).trim().toLowerCase();
  const repo = AppDataSource.getRepository(Users);

  const user = await repo.findOne({ where: { email: normalizedEmail } as any });
  if (!user) return res.status(401).json({ message: 'Invalid credentials' });

  const ok = await bcrypt.compare(String(password), user.password);
  if (!ok) return res.status(401).json({ message: 'Invalid credentials' });

  const roles = normalizeRoles(user.roles);
  const token = jwt.sign({ userId: user.id, email: user.email, roles }, JWT_SECRET, { expiresIn: '1d' });

  return res.json({
    token,
    user: {
      id: user.id,
      email: user.email,
      name: user.name,
      phone: user.phone,
      roles,
      company_id: user.company_id,
    },
  });
});

usersRouter.use(authMiddleware);

async function getAuthUserId(req: Request): Promise<number | null> {
  const authUser = req.user as any;
  const userIdRaw = authUser?.userId;
  const id = Number(userIdRaw);
  if (!userIdRaw || !Number.isInteger(id) || id <= 0) return null;
  return id;
}

usersRouter.get('/me', async (req: Request, res: Response) => {
  const id = await getAuthUserId(req);
  if (!id) {
    return res.status(401).json({ message: 'Token inválido. Não autenticado.' });
  }

  const repo = AppDataSource.getRepository(Users);
  const user = await repo.findOne({ where: { id } as any, relations: { company: true } });
  if (!user) return res.status(404).json({ message: 'User not found' });

  const roles = normalizeRoles(user.roles);
  return res.json({
    id: user.id,
    email: user.email,
    name: user.name,
    phone: user.phone,
    roles,
    company_id: user.company_id,
    company: user.company ? { id: user.company.id, name: user.company.name } : null,
  });
});
