import { Router, Request, Response } from 'express';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcryptjs';
import type { EntityManager } from 'typeorm';
import { AppDataSource } from '../data-source';
import { Users } from '../entities/Users';
import { Companies } from '../entities/Companies';
import { CompanyUsers } from '../entities/CompanyUsers';
import { authMiddleware } from '../middleware/auth';

export const usersRouter = Router();

const JWT_SECRET = process.env.JWT_SECRET || 'changeme';

function rolesFromType(type: unknown): string[] {
  if (type === 'admin') return ['admin'];
  if (type === 'user') return ['user'];
  return [];
}

usersRouter.post('/register', async (req: Request, res: Response) => {
  const { email, password, name, company_name, site, company_site } = req.body as {
    email?: string;
    password?: string;
    name?: string;
    company_name?: string;
    site?: string;
    company_site?: string;
  };

  if (!email || !password) return res.status(400).json({ message: 'Email and password required' });
  if (!name || !String(name).trim()) return res.status(400).json({ message: 'Name required' });
  if (!company_name || !String(company_name).trim()) return res.status(400).json({ message: 'Company name required' });
  const siteValue = String(site ?? company_site ?? '').trim();
  if (!siteValue) return res.status(400).json({ message: 'Company site required' });

  const normalizedEmail = String(email).trim().toLowerCase();
  if (String(password).length < 8) return res.status(400).json({ message: 'Password must be at least 8 characters' });

  const hash = await bcrypt.hash(String(password), 10);
  const companyName = String(company_name).trim();

  try {
    const result = await AppDataSource.transaction(async (manager: EntityManager) => {
      const usersTx = manager.getRepository(Users);
      const companiesTx = manager.getRepository(Companies);
      const companyUsersTx = manager.getRepository(CompanyUsers);

      const existing = await usersTx.findOne({ where: { email: normalizedEmail } as any });
      if (existing) return { conflict: true as const };

      const company = companiesTx.create({ name: companyName, site: siteValue });
      await companiesTx.save(company);

      const user = usersTx.create({
        email: normalizedEmail,
        password: hash,
        name: String(name).trim(),
        type: 'user',
      });
      await usersTx.save(user);

      const companyUser = companyUsersTx.create({ company_id: company.id, user_id: user.id, owner: true });
      await companyUsersTx.save(companyUser);

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
      type: user.type,
      company_id: company.id,
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

  // Se o usuário antigo não tem password (schema antigo), não dá pra autenticar.
  if (!user.password) return res.status(401).json({ message: 'Invalid credentials' });

  const ok = await bcrypt.compare(String(password), user.password);
  if (!ok) return res.status(401).json({ message: 'Invalid credentials' });

  const roles = rolesFromType(user.type);
  const token = jwt.sign({ userId: user.id, email: user.email, roles }, JWT_SECRET, { expiresIn: '1d' });

  return res.json({
    token,
    user: {
      id: user.id,
      email: user.email,
      name: user.name,
      type: user.type,
      roles,
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

  const headerCompanyIdRaw = (req.headers['x-company-id'] ?? req.headers['X-Company-Id']) as any;
  const headerCompanyId = headerCompanyIdRaw ? Number(Array.isArray(headerCompanyIdRaw) ? headerCompanyIdRaw[0] : headerCompanyIdRaw) : null;
  const headerCompanyIdValid = Number.isInteger(headerCompanyId) && (headerCompanyId as number) > 0 ? (headerCompanyId as number) : null;

  const repo = AppDataSource.getRepository(Users);
  const user = await repo.findOne({ where: { id } as any });
  if (!user) return res.status(404).json({ message: 'User not found' });

  let cu = null as any;
  if (user.type === 'admin' && headerCompanyIdValid) {
    const company = await AppDataSource.getRepository(Companies).findOne({ where: { id: headerCompanyIdValid } as any });
    cu = company ? { company_id: company.id, company } : null;
  } else {
    if (headerCompanyIdValid) {
      cu = await AppDataSource.getRepository(CompanyUsers).findOne({
        where: { user_id: user.id, company_id: headerCompanyIdValid } as any,
        relations: { company: true },
      });
    }
    if (!cu) {
      cu = await AppDataSource.getRepository(CompanyUsers).findOne({
        where: { user_id: user.id } as any,
        relations: { company: true },
      });
    }
  }
  const roles = rolesFromType(user.type);
  const company = cu?.company ?? null;
  const company_id = cu?.company_id ?? company?.id ?? null;

  return res.json({
    id: user.id,
    email: user.email,
    name: user.name,
    type: user.type,
    roles,
    company_id,
    company: company ? { id: company.id, name: company.name, site: company.site, group_id: company.group_id ?? null } : null,
  });
});
