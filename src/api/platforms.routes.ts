import { Router, Request, Response } from 'express';
import { authMiddleware } from '../middleware/auth';
import { AppDataSource } from '../data-source';
import { Platforms } from '../entities/Platforms';

export const platformsRouter = Router();

platformsRouter.use(authMiddleware);

platformsRouter.get('/', async (_req: Request, res: Response) => {
  const list = await AppDataSource.getRepository(Platforms).find({ order: { name: 'ASC' } as any });
  return res.json(
    list.map((p) => ({
      id: p.id,
      type: p.type,
      slug: p.slug,
      name: p.name,
      parameters: p.parameters,
    })),
  );
});


