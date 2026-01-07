import { Router, Request, Response } from 'express';
import { authMiddleware } from '../middleware/auth';
import { AppDataSource } from '../data-source';
import { Groups } from '../entities/Groups';
import { Companies } from '../entities/Companies';
import { CompanyUsers } from '../entities/CompanyUsers';
import { Users } from '../entities/Users';

export const groupsRouter = Router();

groupsRouter.use(authMiddleware);

async function getAuthUserId(req: Request): Promise<number | null> {
  const authUser = req.user as any;
  const userIdRaw = authUser?.userId;
  const id = Number(userIdRaw);
  if (!userIdRaw || !Number.isInteger(id) || id <= 0) return null;
  return id;
}

async function userHasAccessToGroup(userId: number, groupId: number): Promise<boolean> {
  const count = await AppDataSource.getRepository(Companies)
    .createQueryBuilder('c')
    .innerJoin(CompanyUsers, 'cu', 'cu.company_id = c.id')
    .where('cu.user_id = :userId', { userId })
    .andWhere('c.group_id = :groupId', { groupId })
    .getCount();
  return count > 0;
}

async function isAdmin(userId: number): Promise<boolean> {
  const u = await AppDataSource.getRepository(Users).findOne({ where: { id: userId } as any });
  return u?.type === 'admin';
}

groupsRouter.post('/', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const name = String((req.body as any)?.name ?? '').trim();
  if (!name) return res.status(400).json({ message: 'Name required' });

  const group = AppDataSource.getRepository(Groups).create({ name });
  const saved = await AppDataSource.getRepository(Groups).save(group);
  return res.status(201).json(saved);
});

groupsRouter.get('/:id', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ message: 'id inválido' });

  const ok = (await isAdmin(userId)) || (await userHasAccessToGroup(userId, id));
  if (!ok) return res.status(403).json({ message: 'Sem acesso ao grupo' });

  const group = await AppDataSource.getRepository(Groups).findOne({ where: { id } as any });
  if (!group) return res.status(404).json({ message: 'Group not found' });

  return res.json(group);
});

groupsRouter.put('/:id', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ message: 'id inválido' });

  const ok = (await isAdmin(userId)) || (await userHasAccessToGroup(userId, id));
  if (!ok) return res.status(403).json({ message: 'Sem acesso ao grupo' });

  const repo = AppDataSource.getRepository(Groups);
  const group = await repo.findOne({ where: { id } as any });
  if (!group) return res.status(404).json({ message: 'Group not found' });

  const name = String((req.body as any)?.name ?? '').trim();
  if (!name) return res.status(400).json({ message: 'Name required' });

  group.name = name;
  const saved = await repo.save(group);
  return res.json(saved);
});


