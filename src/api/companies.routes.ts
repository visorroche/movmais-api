import { Router, Request, Response } from 'express';
import { authMiddleware } from '../middleware/auth';
import { AppDataSource } from '../data-source';
import { Users } from '../entities/Users';
import { Companies } from '../entities/Companies';

export const companiesRouter = Router();

companiesRouter.use(authMiddleware);

async function getAuthUserId(req: Request): Promise<number | null> {
  const authUser = req.user as any;
  const userIdRaw = authUser?.userId;
  const id = Number(userIdRaw);
  if (!userIdRaw || !Number.isInteger(id) || id <= 0) return null;
  return id;
}

async function getAuthCompanyId(req: Request): Promise<number | null> {
  const userId = await getAuthUserId(req);
  if (!userId) return null;

  const user = await AppDataSource.getRepository(Users).findOne({ where: { id: userId } as any });
  const companyId = Number(user?.company_id);
  if (!Number.isInteger(companyId) || companyId <= 0) return null;
  return companyId;
}

companiesRouter.get('/me', async (req: Request, res: Response) => {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const company = await AppDataSource.getRepository(Companies).findOne({ where: { id: companyId } as any });
  if (!company) return res.status(404).json({ message: 'Company not found' });

  return res.json(company);
});

companiesRouter.put('/me', async (req: Request, res: Response) => {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const repo = AppDataSource.getRepository(Companies);
  const existing = await repo.findOne({ where: { id: companyId } as any });
  if (!existing) return res.status(404).json({ message: 'Company not found' });

  const {
    name,
    document,
    commercial_email,
    commercial_phone,
    site,
    logo,
    highlight_color,
    presentation_text,
  } = req.body as Partial<Companies>;

  if (name !== undefined && !String(name).trim()) return res.status(400).json({ message: 'Name cannot be empty' });

  existing.name = name !== undefined ? String(name).trim() : existing.name;
  existing.document = document !== undefined ? (String(document).trim() || undefined) : existing.document;
  existing.commercial_email = commercial_email !== undefined ? (String(commercial_email).trim() || undefined) : existing.commercial_email;
  existing.commercial_phone = commercial_phone !== undefined ? (String(commercial_phone).trim() || undefined) : existing.commercial_phone;
  existing.site = site !== undefined ? (String(site).trim() || undefined) : existing.site;
  existing.logo = logo !== undefined ? (String(logo).trim() || undefined) : existing.logo;
  existing.highlight_color = highlight_color !== undefined ? (String(highlight_color).trim() || undefined) : existing.highlight_color;
  existing.presentation_text =
    presentation_text !== undefined ? (String(presentation_text).trim() || undefined) : existing.presentation_text;

  const saved = await repo.save(existing);
  return res.json(saved);
});
