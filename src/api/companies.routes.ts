import { Router, Request, Response } from 'express';
import { authMiddleware } from '../middleware/auth';
import { AppDataSource } from '../data-source';
import { Companies } from '../entities/Companies';
import { CompanyUsers } from '../entities/CompanyUsers';
import { Groups } from '../entities/Groups';
import { Users } from '../entities/Users';
import bcrypt from 'bcryptjs';
import { CompanyPlatforms } from '../entities/CompanyPlatforms';
import { Platforms } from '../entities/Platforms';
import { Customers } from '../entities/Customers';
import { Orders } from '../entities/Orders';
import { OrderItems } from '../entities/OrderItems';
import { Products } from '../entities/Products';

export const companiesRouter = Router();

companiesRouter.use(authMiddleware);

function parseCompanyIdFromHeader(req: Request): number | null {
  const raw = (req.headers['x-company-id'] ?? req.headers['X-Company-Id']) as any;
  if (raw === undefined || raw === null || raw === '') return null;
  const n = Number(Array.isArray(raw) ? raw[0] : raw);
  if (!Number.isInteger(n) || n <= 0) return null;
  return n;
}

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

  const authUser = await AppDataSource.getRepository(Users).findOne({ where: { id: userId } as any });
  const isAdmin = authUser?.type === 'admin';

  const headerCompanyId = parseCompanyIdFromHeader(req);
  if (headerCompanyId) {
    if (isAdmin) return headerCompanyId;
    const hasAccess = await AppDataSource.getRepository(CompanyUsers).findOne({
      where: { user_id: userId, company_id: headerCompanyId } as any,
    });
    if (hasAccess) return headerCompanyId;
  }

  if (isAdmin) {
    const first = await AppDataSource.getRepository(Companies)
      .createQueryBuilder('c')
      .leftJoin('c.group', 'g')
      .orderBy('g.name', 'ASC', 'NULLS LAST')
      .addOrderBy('c.name', 'ASC')
      .select(['c.id'])
      .getOne();
    return first?.id ?? null;
  }

  const cu = await AppDataSource.getRepository(CompanyUsers).findOne({ where: { user_id: userId } as any });
  const companyId = Number(cu?.company_id);
  if (!Number.isInteger(companyId) || companyId <= 0) return null;
  return companyId;
}

async function getAuthCompanyGroupFilter(
  req: Request,
): Promise<{ companyId: number; groupId: number | null } | null> {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return null;
  const company = await AppDataSource.getRepository(Companies).findOne({ where: { id: companyId } as any });
  if (!company) return null;
  const groupId = company.group_id ?? null;
  return { companyId: company.id, groupId };
}

companiesRouter.get('/me/dashboard/filters', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const condSqlProducts = groupId ? 'c.group_id = $1' : 'p.company_id = $1';
  const param = groupId ?? companyId;

  const [stores, statuses, channels, categories, states, cities] = await Promise.all([
    AppDataSource.query(
      groupId
        ? `SELECT c.id, c.name FROM companies c WHERE c.group_id = $1 ORDER BY c.name ASC`
        : `SELECT c.id, c.name FROM companies c WHERE c.id = $1`,
      [param],
    ),
    AppDataSource.query(
      `SELECT DISTINCT o.current_status AS value
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql} AND o.current_status IS NOT NULL
       ORDER BY value ASC`,
      [param],
    ),
    AppDataSource.query(
      `SELECT DISTINCT UPPER(TRIM(fq.channel)) AS value
       FROM freight_quotes fq
       JOIN companies c ON c.id = fq.company_id
       WHERE ${groupId ? 'c.group_id = $1' : 'fq.company_id = $1'}
         AND fq.channel IS NOT NULL
         AND TRIM(fq.channel) <> ''
       ORDER BY value ASC`,
      [param],
    ),
    AppDataSource.query(
      `SELECT DISTINCT COALESCE(p.final_category, p.category) AS value
       FROM products p
       JOIN companies c ON c.id = p.company_id
       WHERE ${condSqlProducts} AND COALESCE(p.final_category, p.category) IS NOT NULL
       ORDER BY value ASC`,
      [param],
    ),
    AppDataSource.query(
      `SELECT DISTINCT o.delivery_state AS value
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql} AND o.delivery_state IS NOT NULL
       ORDER BY value ASC`,
      [param],
    ),
    AppDataSource.query(
      `SELECT DISTINCT o.delivery_city AS value
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql} AND o.delivery_city IS NOT NULL
       ORDER BY value ASC`,
      [param],
    ),
  ]);

  return res.json({
    companyId,
    groupId,
    stores: (stores || []).map((r: any) => ({ id: Number(r.id), name: String(r.name) })),
    statuses: (statuses || []).map((r: any) => String(r.value)),
    channels: (channels || []).map((r: any) => String(r.value)),
    categories: (categories || []).map((r: any) => String(r.value)),
    states: (states || []).map((r: any) => String(r.value)),
    cities: (cities || []).map((r: any) => String(r.value)),
  });
});

// Log de integrações (executados pelo script-bi/scheduler)
companiesRouter.get('/me/integration-logs', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const { companyId, groupId } = filter;

  const q = req.query as Record<string, unknown>;
  const command = typeof q.command === 'string' && q.command.trim() ? q.command.trim() : null;
  const platformId = typeof q.platform_id === 'string' ? Number(q.platform_id) : null;

  const startDate = typeof q['start-date'] === 'string' ? String(q['start-date']).trim() : '';
  const endDate = typeof q['end-date'] === 'string' ? String(q['end-date']).trim() : '';
  const processedStart = typeof q['processed-start'] === 'string' ? String(q['processed-start']).trim() : '';
  const processedEnd = typeof q['processed-end'] === 'string' ? String(q['processed-end']).trim() : '';

  const limitRaw = typeof q.limit === 'string' ? Number(q.limit) : 50;
  const offsetRaw = typeof q.offset === 'string' ? Number(q.offset) : 0;
  const limit = Number.isInteger(limitRaw) ? Math.max(1, Math.min(200, limitRaw)) : 50;
  const offset = Number.isInteger(offsetRaw) ? Math.max(0, offsetRaw) : 0;

  const where: string[] = [];
  const params: any[] = [];

  // Escopo de acesso: company atual ou grupo
  if (groupId) {
    params.push(groupId);
    where.push(`c.group_id = $${params.length}`);
  } else {
    params.push(companyId);
    where.push(`l.company_id = $${params.length}`);
  }

  if (command) {
    params.push(command);
    where.push(`l.command = $${params.length}`);
  }
  if (platformId && Number.isInteger(platformId) && platformId > 0) {
    params.push(platformId);
    where.push(`l.platform_id = $${params.length}`);
  }

  if (startDate) {
    params.push(startDate);
    where.push(`l.date >= $${params.length}::date`);
  }
  if (endDate) {
    params.push(endDate);
    where.push(`l.date <= $${params.length}::date`);
  }

  if (processedStart) {
    params.push(processedStart);
    where.push(`l.processed_at >= $${params.length}::timestamptz`);
  }
  if (processedEnd) {
    params.push(processedEnd);
    where.push(`l.processed_at <= $${params.length}::timestamptz`);
  }

  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';

  const totalRows = await AppDataSource.query(
    `
    SELECT COUNT(*)::int AS total
    FROM logs l
    JOIN companies c ON c.id = l.company_id
    LEFT JOIN platforms p ON p.id = l.platform_id
    ${whereSql}
  `,
    params,
  );
  const total = Number(totalRows?.[0]?.total ?? 0) || 0;

  const rows = await AppDataSource.query(
    `
    SELECT
      l.id,
      l.processed_at,
      l.date,
      l.company_id,
      c.name AS company_name,
      l.platform_id,
      p.name AS platform_name,
      p.slug AS platform_slug,
      l.command,
      l.log,
      l.errors
    FROM logs l
    JOIN companies c ON c.id = l.company_id
    LEFT JOIN platforms p ON p.id = l.platform_id
    ${whereSql}
    ORDER BY l.processed_at DESC, l.id DESC
    LIMIT ${limit} OFFSET ${offset}
  `,
    params,
  );

  return res.json({ total, items: rows });
});

companiesRouter.get('/me/dashboard/products', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const { companyId, groupId } = filter;
  const q = String((req.query as any)?.q ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  const repo = AppDataSource.getRepository(Products);
  const qb = repo.createQueryBuilder('p');

  if (groupId) {
    qb.innerJoin(Companies, 'c', 'c.id = p.company_id').where('c.group_id = :groupId', { groupId });
  } else {
    qb.where('p.company_id = :companyId', { companyId });
  }

  if (q) {
    qb.andWhere(`(CAST(p.sku AS text) ILIKE :q OR p.name ILIKE :q)`, { q: `%${q}%` });
  }

  const rows = await qb
    .orderBy('p.id', 'DESC')
    .limit(limit)
    .getMany();

  return res.json(
    rows.map((p) => ({
      id: p.id,
      sku: p.sku,
      name: p.name ?? null,
      brand: p.brand ?? null,
      model: p.model ?? null,
      category: p.final_category ?? p.category ?? null,
    })),
  );
});

companiesRouter.get('/me/dashboard/cities', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const param = groupId ?? companyId;

  const statesRaw = ((req.query as any)?.state ?? []) as any;
  const states = (Array.isArray(statesRaw) ? statesRaw : [statesRaw])
    .map((s) => String(s || '').trim())
    .filter(Boolean);

  const rows = await AppDataSource.query(
    `SELECT DISTINCT o.delivery_city AS value
     FROM orders o
     JOIN companies c ON c.id = o.company_id
     WHERE ${condSql}
       AND o.delivery_city IS NOT NULL
       AND (CASE WHEN $2::text[] IS NULL OR array_length($2::text[], 1) IS NULL THEN TRUE ELSE o.delivery_state = ANY($2::text[]) END)
     ORDER BY value ASC`,
    [param, states.length ? states : null],
  );

  return res.json((rows || []).map((r: any) => String(r.value)));
});

function isIsoYmd(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

companiesRouter.get('/me/dashboard/revenue', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const param = groupId ?? companyId;

  const condSqlFq = groupId ? 'c.group_id = $1' : 'fq.company_id = $1';

  const [todayRows, periodRows, dailyRows, ordersCountRows, quotesCountRows] = await Promise.all([
    AppDataSource.query(
      `SELECT COALESCE(SUM(o.total_amount), 0) AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql}
         AND o.order_date IS NOT NULL
         AND o.order_date::date = CURRENT_DATE`,
      [param],
    ),
    AppDataSource.query(
      `SELECT COALESCE(SUM(o.total_amount), 0) AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql}
         AND o.order_date IS NOT NULL
         AND o.order_date::date BETWEEN $2::date AND $3::date`,
      [param, start, end],
    ),
    AppDataSource.query(
      `SELECT
         o.order_date::date AS day_date,
         to_char(o.order_date::date, 'DD/MM/YYYY') AS day,
         COALESCE(SUM(o.total_amount), 0) AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql}
         AND o.order_date IS NOT NULL
         AND o.order_date::date BETWEEN $2::date AND $3::date
       GROUP BY 1, 2
       ORDER BY 1 ASC`,
      [param, start, end],
    ),
    AppDataSource.query(
      `SELECT COUNT(*) AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql}
         AND o.order_date IS NOT NULL
         AND o.order_date::date BETWEEN $2::date AND $3::date`,
      [param, start, end],
    ),
    AppDataSource.query(
      `SELECT COUNT(*) AS total
       FROM freight_quotes fq
       JOIN companies c ON c.id = fq.company_id
       WHERE ${condSqlFq}
         AND fq.quoted_at IS NOT NULL
         AND fq.quoted_at::date BETWEEN $2::date AND $3::date`,
      [param, start, end],
    ),
  ]);

  const todayTotal = Number((todayRows?.[0] as any)?.total ?? 0) || 0;
  const periodTotal = Number((periodRows?.[0] as any)?.total ?? 0) || 0;
  const ordersCount = Number((ordersCountRows?.[0] as any)?.total ?? 0) || 0;
  const quotesCount = Number((quotesCountRows?.[0] as any)?.total ?? 0) || 0;
  const conversionRate = quotesCount > 0 ? ordersCount / quotesCount : 0;
  const daily = (dailyRows || []).map((r: any) => ({
    date: String(r.day), // DD/MM/YYYY (para o gráfico)
    total: Number(r.total ?? 0) || 0,
  }));

  return res.json({
    companyId,
    groupId,
    start,
    end,
    today: todayTotal,
    period: periodTotal,
    ordersCount,
    quotesCount,
    conversionRate,
    daily,
  });
});

companiesRouter.get('/me/dashboard/simulations/daily', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const { companyId, groupId } = filter;
  const param = groupId ?? companyId;
  const condQuotes = groupId ? 'c.group_id = $1' : 'fq.company_id = $1';
  const condFo = groupId ? 'c2.group_id = $1' : 'fo.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const channels = toStringArray((req.query as any)?.channel);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const skus = toStringArray((req.query as any)?.sku);
  const storeNames = toStringArray((req.query as any)?.store_name);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  try {
    const params: any[] = [param, start, end];
    const where: string[] = [
      `${condQuotes}`,
      `fq.quoted_at IS NOT NULL`,
      `fq.quoted_at::date BETWEEN $2::date AND $3::date`,
    ];
    if (channels.length) {
      params.push(channels);
      where.push(`fq.channel = ANY($${params.length}::text[])`);
    }
    if (states.length) {
      params.push(states);
      where.push(`UPPER(TRIM(fq.destination_state)) = ANY($${params.length}::text[])`);
    }
    // filtro opcional por company_id (útil quando o usuário está no escopo de group e seleciona "lojas/companies")
    if (companyIds.length) {
      params.push(companyIds);
      where.push(`fq.company_id = ANY($${params.length}::int[])`);
    }
    if (storeNames.length) {
      params.push(storeNames);
      where.push(`fq.store_name = ANY($${params.length}::text[])`);
    }
    if (skus.length) {
      params.push(skus);
      where.push(`
        EXISTS (
          SELECT 1
          FROM freight_quotes_items fqi
          JOIN products p ON p.id = fqi.product_id
          WHERE fqi.quote_id = fq.id
            AND p.sku = ANY($${params.length}::text[])
        )
      `);
    }

    const rows = await AppDataSource.query(
      `WITH days AS (
         SELECT generate_series($2::date, $3::date, '1 day'::interval)::date AS day
       ),
       base_quotes AS (
         SELECT fq.id, fq.quote_id, fq.quoted_at::date AS day
         FROM freight_quotes fq
         JOIN companies c ON c.id = fq.company_id
         WHERE ${where.join(' AND ')}
       ),
       quotes AS (
         SELECT day, COUNT(DISTINCT quote_id)::int AS total
         FROM base_quotes
         GROUP BY 1
       ),
       orders AS (
         SELECT fo.order_date::date AS day, COUNT(DISTINCT fo.quote_id)::int AS total
         FROM freight_orders fo
         JOIN companies c2 ON c2.id = fo.company_id
         JOIN base_quotes bq ON bq.quote_id = fo.quote_id
         WHERE ${condFo}
           AND fo.order_date IS NOT NULL
           AND fo.order_date::date BETWEEN $2::date AND $3::date
         GROUP BY 1
       )
       SELECT
         to_char(d.day, 'DD/MM/YYYY') AS date,
         COALESCE(q.total, 0)::int AS sims,
         COALESCE(o.total, 0)::int AS orders
       FROM days d
       LEFT JOIN quotes q ON q.day = d.day
       LEFT JOIN orders o ON o.day = d.day
       ORDER BY d.day ASC`,
      params,
    );

    return res.json({
      companyId,
      groupId,
      start,
      end,
      daily: (rows || []).map((r: any) => ({
        date: String(r.date),
        sims: Number(r.sims ?? 0) || 0,
        orders: Number(r.orders ?? 0) || 0,
      })),
    });
  } catch (err: any) {
    // se a tabela freight_quotes ainda não existir no banco, a query falha
    return res.status(500).json({ message: err?.message || 'Erro ao buscar simulações' });
  }
});

companiesRouter.get('/me/dashboard/simulations/freight-scatter', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const limitRaw = Number((req.query as any)?.limit ?? 800);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 50), 2000) : 800;

  const { companyId, groupId } = filter;
  const param = groupId ?? companyId;
  const condSqlFq = groupId ? 'c.group_id = $1' : 'f.company_id = $1';
  const condSqlFo = groupId ? 'c2.group_id = $1' : 'fo.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };
  const channels = toStringArray((req.query as any)?.channel);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const skus = toStringArray((req.query as any)?.sku);
  const storeNames = toStringArray((req.query as any)?.store_name);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  try {
    const params: any[] = [param, start, end, limit];
    const where: string[] = [
      `${condSqlFq}`,
      `f.quoted_at IS NOT NULL`,
      `f.quoted_at::date BETWEEN $2::date AND $3::date`,
      `o.shipping_value IS NOT NULL`,
      `o.deadline IS NOT NULL`,
    ];
    if (channels.length) {
      params.push(channels);
      where.push(`f.channel = ANY($${params.length}::text[])`);
    }
    if (states.length) {
      params.push(states);
      where.push(`UPPER(TRIM(f.destination_state)) = ANY($${params.length}::text[])`);
    }
    if (companyIds.length) {
      params.push(companyIds);
      where.push(`f.company_id = ANY($${params.length}::int[])`);
    }
    if (storeNames.length) {
      params.push(storeNames);
      where.push(`f.store_name = ANY($${params.length}::text[])`);
    }
    if (skus.length) {
      params.push(skus);
      where.push(`
        EXISTS (
          SELECT 1
          FROM freight_quotes_items fqi
          JOIN products p ON p.id = fqi.product_id
          WHERE fqi.quote_id = f.id
            AND p.sku = ANY($${params.length}::text[])
        )
      `);
    }

    const rows = await AppDataSource.query(
      `WITH opt_ranked AS (
        SELECT
          f.quote_id,
          o.shipping_value,
          o.deadline,
          ROW_NUMBER() OVER (
            PARTITION BY f.quote_id
            ORDER BY o.shipping_value ASC NULLS LAST, o.deadline ASC NULLS LAST
          ) AS rn
        FROM freight_quotes f
        JOIN companies c ON c.id = f.company_id
        JOIN freight_quote_options o ON o.freight_quote_id = f.id
        WHERE ${where.join(' AND ')}
      ),
      one_option_per_quote AS (
        SELECT
          r.quote_id,
          r.shipping_value,
          r.deadline,
          CASE
            WHEN EXISTS (
              SELECT 1
              FROM freight_orders fo
              LEFT JOIN companies c2 ON c2.id = fo.company_id
              WHERE fo.quote_id = r.quote_id
                AND (${condSqlFo})
              LIMIT 1
            )
            THEN 1
            ELSE 0
          END AS is_converted
        FROM opt_ranked r
        WHERE r.rn = 1
      ),
      bucketed AS (
        SELECT
          CASE
            WHEN shipping_value = 0 THEN 'R$0,00 (FREE)'
            WHEN shipping_value BETWEEN 0.01 AND 100.00 THEN 'entre R$ 0,01 e R$ 100,00'
            WHEN shipping_value BETWEEN 100.01 AND 200.00 THEN 'entre R$ 100,01 e R$ 200,00'
            WHEN shipping_value BETWEEN 200.01 AND 300.00 THEN 'entre R$ 200,01 e R$ 300,00'
            WHEN shipping_value BETWEEN 300.01 AND 500.00 THEN 'entre R$ 300,01 e R$ 500,00'
            WHEN shipping_value BETWEEN 500.01 AND 1000.00 THEN 'entre R$ 500,01 e R$ 1.000,00'
            WHEN shipping_value BETWEEN 1000.01 AND 10000.00 THEN 'entre R$ 1.000,01 e R$ 10.000,00'
            ELSE 'acima de R$ 10.000,00'
          END AS range_value,
          CASE
            WHEN deadline <= 0 THEN '>0'
            WHEN deadline <= 5 THEN '>0'
            WHEN deadline <= 10 THEN '>5'
            WHEN deadline <= 15 THEN '>10'
            WHEN deadline <= 20 THEN '>15'
            WHEN deadline <= 25 THEN '>20'
            WHEN deadline <= 30 THEN '>25'
            WHEN deadline <= 35 THEN '>30'
            WHEN deadline <= 40 THEN '>35'
            WHEN deadline <= 45 THEN '>40'
            WHEN deadline <= 60 THEN '>45'
            ELSE '>60'
          END AS range_deadline,
          is_converted
        FROM one_option_per_quote
      ),
      agg AS (
        SELECT
          range_value,
          range_deadline,
          COUNT(*)::int AS total,
          SUM(is_converted)::int AS orders,
          (SUM(is_converted)::numeric / NULLIF(COUNT(*)::numeric, 0)) AS conv
        FROM bucketed
        GROUP BY 1, 2
      )
      SELECT range_value, range_deadline, total, orders, conv
      FROM agg
      ORDER BY total DESC
      LIMIT $4`,
      params,
    );

    return res.json({
      companyId,
      groupId,
      start,
      end,
      points: (rows || []).map((r: any) => ({
        rangeValue: String(r.range_value),
        rangeDeadline: String(r.range_deadline),
        total: Number(r.total ?? 0) || 0,
        orders: Number(r.orders ?? 0) || 0,
        conversion: Number(r.conv ?? 0) || 0,
      })),
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao carregar mapa de conversão (scatter)' });
  }
});

companiesRouter.get('/me/dashboard/simulations/by-state', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const { companyId, groupId } = filter;
  const param = groupId ?? companyId;
  const condSqlFq = groupId ? 'c.group_id = $1' : 'f.company_id = $1';
  const condSqlFo = groupId ? 'c2.group_id = $1' : 'fo.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };
  const channels = toStringArray((req.query as any)?.channel);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const skus = toStringArray((req.query as any)?.sku);
  const storeNames = toStringArray((req.query as any)?.store_name);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  try {
    const params: any[] = [param, start, end];
    const where: string[] = [
      `${condSqlFq}`,
      `f.quoted_at IS NOT NULL`,
      `f.quoted_at::date BETWEEN $2::date AND $3::date`,
      `f.destination_state IS NOT NULL`,
      `TRIM(f.destination_state) <> ''`,
    ];
    if (channels.length) {
      params.push(channels);
      where.push(`f.channel = ANY($${params.length}::text[])`);
    }
    if (states.length) {
      params.push(states);
      where.push(`UPPER(TRIM(f.destination_state)) = ANY($${params.length}::text[])`);
    }
    if (companyIds.length) {
      params.push(companyIds);
      where.push(`f.company_id = ANY($${params.length}::int[])`);
    }
    if (storeNames.length) {
      params.push(storeNames);
      where.push(`f.store_name = ANY($${params.length}::text[])`);
    }
    if (skus.length) {
      params.push(skus);
      where.push(`
        EXISTS (
          SELECT 1
          FROM freight_quotes_items fqi
          JOIN products p ON p.id = fqi.product_id
          WHERE fqi.quote_id = f.id
            AND p.sku = ANY($${params.length}::text[])
        )
      `);
    }

    const rows = await AppDataSource.query(
      `
      SELECT
        UPPER(TRIM(f.destination_state)) AS state,
        COUNT(DISTINCT f.quote_id)::int AS sims,
        COUNT(DISTINCT CASE
          WHEN EXISTS (
            SELECT 1
            FROM freight_orders fo
            LEFT JOIN companies c2 ON c2.id = fo.company_id
            WHERE fo.quote_id = f.quote_id
              AND (${condSqlFo})
            LIMIT 1
          )
          THEN f.quote_id
          ELSE NULL
        END)::int AS orders
      FROM freight_quotes f
      JOIN companies c ON c.id = f.company_id
      WHERE ${where.join(' AND ')}
      GROUP BY 1
      ORDER BY sims DESC, state ASC
      `,
      params,
    );

    return res.json({
      companyId,
      groupId,
      start,
      end,
      states: (rows || []).map((r: any) => {
        const sims = Number(r.sims ?? 0) || 0;
        const orders = Number(r.orders ?? 0) || 0;
        return {
          state: String(r.state),
          sims,
          orders,
          conversion: sims > 0 ? orders / sims : 0,
        };
      }),
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao buscar simulações por estado' });
  }
});

companiesRouter.get('/me/customers', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const q = String((req.query as any)?.q ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  const repo = AppDataSource.getRepository(Customers);
  const qb = repo.createQueryBuilder('c').where('c.company_id = :companyId', { companyId });

  if (q) {
    qb.andWhere(
      `(c.tax_id ILIKE :q OR c.legal_name ILIKE :q OR c.trade_name ILIKE :q OR c.email ILIKE :q OR c.external_id ILIKE :q)`,
      { q: `%${q}%` },
    );
  }

  const rows = await qb
    .orderBy('c.id', 'DESC')
    .limit(limit)
    .getMany();

  return res.json(
    rows.map((c) => ({
      id: c.id,
      tax_id: c.tax_id,
      legal_name: c.legal_name ?? null,
      trade_name: c.trade_name ?? null,
      email: c.email ?? null,
      status: c.status ?? null,
      external_id: c.external_id ?? null,
    })),
  );
});

companiesRouter.get('/me/customers/:id', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ message: 'id inválido' });

  const customer = await AppDataSource.getRepository(Customers).findOne({ where: { id, company_id: companyId } as any });
  if (!customer) return res.status(404).json({ message: 'Customer not found' });

  return res.json({
    id: customer.id,
    tax_id: customer.tax_id,
    state_registration: customer.state_registration ?? null,
    person_type: customer.person_type ?? null,
    legal_name: customer.legal_name ?? null,
    trade_name: customer.trade_name ?? null,
    gender: customer.gender ?? null,
    birth_date: customer.birth_date ?? null,
    email: customer.email ?? null,
    status: customer.status ?? null,
    delivery_address: customer.delivery_address ?? null,
    phones: customer.phones ?? null,
    external_id: customer.external_id ?? null,
    company_id: customer.company_id,
    raw: customer.raw ?? null,
  });
});

companiesRouter.get('/me/customers/:id/orders', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const customerId = Number(req.params.id);
  if (!Number.isInteger(customerId) || customerId <= 0) return res.status(400).json({ message: 'id inválido' });

  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  // garante que o customer pertence à company
  const exists = await AppDataSource.getRepository(Customers).exist({ where: { id: customerId, company_id: companyId } as any });
  if (!exists) return res.status(404).json({ message: 'Customer not found' });

  const repo = AppDataSource.getRepository(Orders);
  const rows = await repo
    .createQueryBuilder('o')
    .where('o.company_id = :companyId', { companyId })
    .andWhere('o.customer_id = :customerId', { customerId })
    .select([
      'o.id AS id',
      'o.order_code AS order_code',
      'o.order_date AS order_date',
      'o.current_status AS current_status',
      'o.total_amount AS total_amount',
      'o.marketplace_name AS marketplace_name',
    ])
    .orderBy('o.id', 'DESC')
    .limit(limit)
    .getRawMany<any>();

  return res.json(
    rows.map((r) => ({
      id: Number(r.id),
      order_code: Number(r.order_code),
      order_date: r.order_date ?? null,
      current_status: r.current_status ?? null,
      total_amount: r.total_amount ?? null,
      marketplace_name: r.marketplace_name ?? null,
    })),
  );
});

companiesRouter.get('/me/orders', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const q = String((req.query as any)?.q ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  const repo = AppDataSource.getRepository(Orders);
  const qb = repo
    .createQueryBuilder('o')
    .leftJoin(Customers, 'c', 'c.id = o.customer_id')
    .where('o.company_id = :companyId', { companyId });

  if (q) {
    qb.andWhere(
      `(
        CAST(o.order_code AS text) ILIKE :q OR
        o.partner_order_id ILIKE :q OR
        o.marketplace_name ILIKE :q OR
        o.channel ILIKE :q OR
        o.current_status ILIKE :q OR
        c.tax_id ILIKE :q OR
        c.legal_name ILIKE :q OR
        c.trade_name ILIKE :q OR
        c.email ILIKE :q
      )`,
      { q: `%${q}%` },
    );
  }

  const rows = await qb
    .select([
      'o.id AS id',
      'o.order_code AS order_code',
      'o.order_date AS order_date',
      'o.current_status AS current_status',
      'o.total_amount AS total_amount',
      'o.marketplace_name AS marketplace_name',
      'o.customer_id AS customer_id',
      'c.trade_name AS customer_trade_name',
      'c.legal_name AS customer_legal_name',
      'c.email AS customer_email',
      'c.tax_id AS customer_tax_id',
    ])
    .orderBy('o.id', 'DESC')
    .limit(limit)
    .getRawMany<any>();

  return res.json(
    rows.map((r) => ({
      id: Number(r.id),
      order_code: Number(r.order_code),
      order_date: r.order_date ?? null,
      current_status: r.current_status ?? null,
      total_amount: r.total_amount ?? null,
      marketplace_name: r.marketplace_name ?? null,
      customer: r.customer_id
        ? {
            id: Number(r.customer_id),
            name: r.customer_trade_name ?? r.customer_legal_name ?? null,
            email: r.customer_email ?? null,
            tax_id: r.customer_tax_id ?? null,
          }
        : null,
    })),
  );
});

companiesRouter.get('/me/orders/:id', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ message: 'id inválido' });

  const order = await AppDataSource.getRepository(Orders).findOne({
    where: { id, company_id: companyId } as any,
    relations: { customer: true, platform: true, items: { product: true } as any },
  });
  if (!order) return res.status(404).json({ message: 'Order not found' });

  const items = (order.items || []) as OrderItems[];

  return res.json({
    id: order.id,
    order_code: order.order_code,
    order_date: order.order_date ?? null,
    partner_order_id: order.partner_order_id ?? null,
    current_status: order.current_status ?? null,
    current_status_code: order.current_status_code ?? null,
    shipping_amount: order.shipping_amount ?? null,
    delivery_days: order.delivery_days ?? null,
    delivery_date: order.delivery_date ?? null,
    total_amount: order.total_amount ?? null,
    total_discount: order.total_discount ?? null,
    marketplace_name: order.marketplace_name ?? null,
    channel: order.channel ?? null,
    payment_date: order.payment_date ?? null,
    discount_coupon: order.discount_coupon ?? null,
    delivery_state: order.delivery_state ?? null,
    delivery_zip: order.delivery_zip ?? null,
    delivery_neighborhood: order.delivery_neighborhood ?? null,
    delivery_city: order.delivery_city ?? null,
    delivery_number: order.delivery_number ?? null,
    delivery_address: order.delivery_address ?? null,
    delivery_complement: order.delivery_complement ?? null,
    metadata: order.metadata ?? null,
    store_pickup: order.store_pickup ?? null,
    payments: order.payments ?? null,
    tracking: order.tracking ?? null,
    timeline: order.timeline ?? null,
    raw: order.raw ?? null,
    customer: order.customer
      ? {
          id: order.customer.id,
          tax_id: order.customer.tax_id,
          legal_name: order.customer.legal_name ?? null,
          trade_name: order.customer.trade_name ?? null,
          email: order.customer.email ?? null,
        }
      : null,
    platform: order.platform
      ? { id: order.platform.id, name: order.platform.name, slug: order.platform.slug, type: order.platform.type }
      : null,
    items: items.map((it) => ({
      id: it.id,
      product_id: it.product_id ?? null,
      product: (it as any).product
        ? {
            id: (it as any).product.id,
            name: (it as any).product.name ?? null,
            sku: (it as any).product.sku ?? null,
            brand: (it as any).product.brand ?? null,
            model: (it as any).product.model ?? null,
            category: (it as any).product.final_category ?? (it as any).product.category ?? null,
          }
        : null,
      sku: it.sku ?? null,
      quantity: it.quantity ?? null,
      unit_price: it.unit_price ?? null,
      net_unit_price: it.net_unit_price ?? null,
      item_type: it.item_type ?? null,
      service_ref_sku: it.service_ref_sku ?? null,
    })),
  });
});

companiesRouter.get('/me/products', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const q = String((req.query as any)?.q ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  const repo = AppDataSource.getRepository(Products);
  const qb = repo.createQueryBuilder('p').where('p.company_id = :companyId', { companyId });

  if (q) {
    qb.andWhere(
      `(CAST(p.sku AS text) ILIKE :q OR p.name ILIKE :q OR p.ean ILIKE :q OR p.slug ILIKE :q OR p.external_reference ILIKE :q OR p.store_reference ILIKE :q)`,
      { q: `%${q}%` },
    );
  }

  const rows = await qb
    .orderBy('p.id', 'DESC')
    .limit(limit)
    .getMany();

  return res.json(
    rows.map((p) => ({
      id: p.id,
      sku: p.sku,
      name: p.name ?? null,
      ean: p.ean ?? null,
      brand: p.brand ?? null,
      model: p.model ?? null,
      category: p.final_category ?? p.category ?? null,
    })),
  );
});

companiesRouter.get('/me/products/:id', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const id = Number(req.params.id);
  if (!Number.isInteger(id) || id <= 0) return res.status(400).json({ message: 'id inválido' });

  const product = await AppDataSource.getRepository(Products).findOne({ where: { id, company_id: companyId } as any });
  if (!product) return res.status(404).json({ message: 'Product not found' });

  return res.json({
    id: product.id,
    company_id: product.company_id,
    sku: product.sku,
    ecommerce_id: product.ecommerce_id ?? null,
    ean: product.ean ?? null,
    slug: product.slug ?? null,
    name: product.name ?? null,
    store_reference: product.store_reference ?? null,
    external_reference: product.external_reference ?? null,
    brand_id: product.brand_id ?? null,
    brand: product.brand ?? null,
    model: product.model ?? null,
    weight: product.weight ?? null,
    width: product.width ?? null,
    height: product.height ?? null,
    length_cm: product.length_cm ?? null,
    ncm: product.ncm ?? null,
    category: product.category ?? null,
    category_id: product.category_id ?? null,
    subcategory: product.subcategory ?? null,
    final_category: product.final_category ?? null,
    photo: product.photo ?? null,
    url: product.url ?? null,
    raw: product.raw ?? null,
  });
});

companiesRouter.get('/me/platforms', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const rows = await AppDataSource.getRepository(CompanyPlatforms).find({
    where: { company_id: companyId } as any,
    relations: { platform: true },
  });

  return res.json(
    rows.map((cp) => ({
      id: cp.id,
      company_id: cp.company_id ?? null,
      platform_id: cp.platform_id ?? (cp.platform as any)?.id ?? null,
      config: cp.config ?? {},
      platform: cp.platform
        ? {
            id: cp.platform.id,
            type: cp.platform.type,
            slug: cp.platform.slug,
            name: cp.platform.name,
            parameters: cp.platform.parameters,
          }
        : null,
    })),
  );
});

companiesRouter.put('/me/platforms/:platformId', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const platformId = Number(req.params.platformId);
  if (!Number.isInteger(platformId) || platformId <= 0) return res.status(400).json({ message: 'platformId inválido' });

  const config = (req.body as any)?.config;
  if (config === null || config === undefined || typeof config !== 'object' || Array.isArray(config)) {
    return res.status(400).json({ message: 'config inválido (esperado objeto JSON)' });
  }

  const platform = await AppDataSource.getRepository(Platforms).findOne({ where: { id: platformId } as any });
  if (!platform) return res.status(404).json({ message: 'Platform not found' });

  const saved = await AppDataSource.transaction(async (manager) => {
    const repo = manager.getRepository(CompanyPlatforms);
    const existing = await repo.findOne({ where: { company_id: companyId, platform_id: platformId } as any });
    if (existing) {
      existing.config = config;
      return repo.save(existing);
    }
    const created = repo.create({ company_id: companyId, platform_id: platformId, config });
    return repo.save(created);
  });

  return res.json({
    id: saved.id,
    company_id: saved.company_id ?? null,
    platform_id: saved.platform_id ?? platformId,
    config: saved.config ?? {},
    platform: {
      id: platform.id,
      type: platform.type,
      slug: platform.slug,
      name: platform.name,
      parameters: platform.parameters,
    },
  });
});

companiesRouter.get('/me/users', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const rows = await AppDataSource.getRepository(Users)
    .createQueryBuilder('u')
    .innerJoin(CompanyUsers, 'cu', 'cu.user_id = u.id')
    .where('cu.company_id = :companyId', { companyId })
    .select(['u.id AS id', 'u.name AS name', 'u.email AS email', 'u.type AS type', 'cu.owner AS owner'])
    .orderBy('cu.owner', 'DESC')
    .addOrderBy('u.name', 'ASC')
    .addOrderBy('u.email', 'ASC')
    .getRawMany<{ id: number; name: string; email: string; type: 'admin' | 'user'; owner: boolean }>();

  return res.json(
    rows.map((r) => ({
      id: Number(r.id),
      name: r.name,
      email: r.email,
      type: r.type,
      owner: Boolean((r as any).owner),
    })),
  );
});

companiesRouter.post('/me/users', async (req: Request, res: Response) => {
  const authUserId = await getAuthUserId(req);
  if (!authUserId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const { name, email, password, type } = req.body as {
    name?: string;
    email?: string;
    password?: string;
    type?: 'admin' | 'user';
  };

  const n = String(name ?? '').trim();
  const e = String(email ?? '').trim().toLowerCase();
  const p = String(password ?? '');
  const t = (type === 'admin' || type === 'user') ? type : 'user';

  if (!n) return res.status(400).json({ message: 'Name required' });
  if (!e) return res.status(400).json({ message: 'Email required' });
  if (p.length < 8) return res.status(400).json({ message: 'Password must be at least 8 characters' });

  try {
    const created = await AppDataSource.transaction(async (manager) => {
      const usersRepo = manager.getRepository(Users);
      const cuRepo = manager.getRepository(CompanyUsers);

      const existing = await usersRepo.findOne({ where: { email: e } as any });
      let user: Users;
      if (existing) {
        user = existing;
      } else {
        const hash = await bcrypt.hash(p, 10);
        user = usersRepo.create({ name: n, email: e, password: hash, type: t });
        await usersRepo.save(user);
      }

      const linkExists = await cuRepo.findOne({ where: { company_id: companyId, user_id: user.id } as any });
      if (!linkExists) {
        await cuRepo.save(cuRepo.create({ company_id: companyId, user_id: user.id, owner: false }));
      }

      return user;
    });

    return res.status(201).json({ id: created.id, name: created.name, email: created.email, type: created.type });
  } catch (err: any) {
    if (err?.code === '23505') return res.status(409).json({ message: 'Email already registered' });
    return res.status(500).json({ message: 'Internal server error' });
  }
});

companiesRouter.put('/me/users/:userId', async (req: Request, res: Response) => {
  const authUserId = await getAuthUserId(req);
  if (!authUserId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ message: 'userId inválido' });

  // garante que pertence a essa empresa
  const link = await AppDataSource.getRepository(CompanyUsers).findOne({ where: { company_id: companyId, user_id: userId } as any });
  if (!link) return res.status(404).json({ message: 'User not found in this company' });

  const { name, email, type, password } = req.body as {
    name?: string;
    email?: string;
    type?: 'admin' | 'user';
    password?: string;
  };

  const repo = AppDataSource.getRepository(Users);
  const user = await repo.findOne({ where: { id: userId } as any });
  if (!user) return res.status(404).json({ message: 'User not found' });

  if (name !== undefined) {
    const n = String(name).trim();
    if (!n) return res.status(400).json({ message: 'Name cannot be empty' });
    user.name = n;
  }
  if (email !== undefined) {
    const e = String(email).trim().toLowerCase();
    if (!e) return res.status(400).json({ message: 'Email cannot be empty' });
    user.email = e;
  }
  if (type !== undefined) {
    if (type !== 'admin' && type !== 'user') return res.status(400).json({ message: 'type inválido' });
    user.type = type;
  }
  if (password !== undefined) {
    const p = String(password);
    if (p.length < 8) return res.status(400).json({ message: 'Password must be at least 8 characters' });
    user.password = await bcrypt.hash(p, 10);
  }

  try {
    const saved = await repo.save(user);
    return res.json({ id: saved.id, name: saved.name, email: saved.email, type: saved.type });
  } catch (err: any) {
    if (err?.code === '23505') return res.status(409).json({ message: 'Email already registered' });
    return res.status(500).json({ message: 'Internal server error' });
  }
});

companiesRouter.delete('/me/users/:userId', async (req: Request, res: Response) => {
  const authUserId = await getAuthUserId(req);
  if (!authUserId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const userId = Number(req.params.userId);
  if (!Number.isInteger(userId) || userId <= 0) return res.status(400).json({ message: 'userId inválido' });

  if (userId === authUserId) return res.status(400).json({ message: 'Você não pode remover a si mesmo da empresa' });

  const repo = AppDataSource.getRepository(CompanyUsers);
  const link = await repo.findOne({ where: { company_id: companyId, user_id: userId } as any });
  if (!link) return res.status(404).json({ message: 'User not found in this company' });
  if ((link as any).owner) return res.status(400).json({ message: 'Não é possível remover o owner da empresa' });

  await repo.remove(link);
  return res.json({ ok: true });
});

companiesRouter.get('/my', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const authUser = await AppDataSource.getRepository(Users).findOne({ where: { id: userId } as any });
  const isAdmin = authUser?.type === 'admin';

  const qb = AppDataSource.getRepository(Companies)
    .createQueryBuilder('c')
    .leftJoinAndSelect('c.group', 'g')
    .orderBy('g.name', 'ASC', 'NULLS LAST')
    .addOrderBy('c.name', 'ASC');

  if (!isAdmin) {
    qb.innerJoin(CompanyUsers, 'cu', 'cu.company_id = c.id').where('cu.user_id = :userId', { userId });
  }

  const companies = await qb.getMany();

  return res.json(
    companies.map((c) => ({
      id: c.id,
      name: c.name,
      site: c.site,
      group_id: c.group_id ?? null,
      group: c.group ? { id: c.group.id, name: c.group.name } : null,
    })),
  );
});

companiesRouter.get('/group/:groupId', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const groupId = Number(req.params.groupId);
  if (!Number.isInteger(groupId) || groupId <= 0) return res.status(400).json({ message: 'groupId inválido' });

  const authUser = await AppDataSource.getRepository(Users).findOne({ where: { id: userId } as any });
  const isAdmin = authUser?.type === 'admin';

  const qb = AppDataSource.getRepository(Companies)
    .createQueryBuilder('c')
    .where('c.group_id = :groupId', { groupId });

  if (!isAdmin) {
    qb.innerJoin(CompanyUsers, 'cu', 'cu.company_id = c.id').andWhere('cu.user_id = :userId', { userId });
  }

  const companies = await qb.orderBy('c.name', 'ASC').getMany();

  return res.json(companies);
});

companiesRouter.get('/me', async (req: Request, res: Response) => {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const company = await AppDataSource.getRepository(Companies).findOne({ where: { id: companyId } as any });
  if (!company) return res.status(404).json({ message: 'Company not found' });

  return res.json(company);
});

companiesRouter.post('/', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const currentCompanyId = await getAuthCompanyId(req);
  if (!currentCompanyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const { name, site, grant_all_users } = req.body as {
    name?: string;
    site?: string;
    grant_all_users?: boolean;
  };

  const companyName = String(name ?? '').trim();
  const companySite = String(site ?? '').trim();
  if (!companyName) return res.status(400).json({ message: 'Name cannot be empty' });
  if (!companySite) return res.status(400).json({ message: 'Site cannot be empty' });

  try {
    const created = await AppDataSource.transaction(async (manager) => {
      const companiesRepo = manager.getRepository(Companies);
      const cuRepo = manager.getRepository(CompanyUsers);

      const currentCompany = await companiesRepo.findOne({ where: { id: currentCompanyId } as any });
      if (!currentCompany) throw Object.assign(new Error('Company not found'), { status: 404 });
      const groupId = currentCompany.group_id;
      if (!groupId) throw Object.assign(new Error('Empresa atual não possui grupo'), { status: 400 });

      const newCompany = companiesRepo.create({ name: companyName, site: companySite, group_id: groupId });
      await companiesRepo.save(newCompany);

      let userIds: number[] = [];
      if (grant_all_users) {
        const rows = await cuRepo
          .createQueryBuilder('cu')
          .select('DISTINCT cu.user_id', 'user_id')
          .where('cu.company_id = :companyId', { companyId: currentCompanyId })
          .andWhere('cu.user_id IS NOT NULL')
          .getRawMany<{ user_id: number }>();
        userIds = rows.map((r) => Number(r.user_id)).filter((n) => Number.isInteger(n) && n > 0);
      } else {
        userIds = [userId];
      }

      // Evita duplicados (mesmo não tendo constraint única)
      const uniqueUserIds = Array.from(new Set(userIds));
      for (const uid of uniqueUserIds) {
        const exists = await cuRepo.findOne({ where: { company_id: newCompany.id, user_id: uid } as any });
        if (exists) continue;
        await cuRepo.save(cuRepo.create({ company_id: newCompany.id, user_id: uid, owner: uid === userId }));
      }

      return newCompany;
    });

    return res.status(201).json(created);
  } catch (err: any) {
    const status = err?.status;
    if (status) return res.status(status).json({ message: err.message });
    return res.status(500).json({ message: 'Internal server error' });
  }
});

companiesRouter.put('/me', async (req: Request, res: Response) => {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const repo = AppDataSource.getRepository(Companies);
  const existing = await repo.findOne({ where: { id: companyId } as any });
  if (!existing) return res.status(404).json({ message: 'Company not found' });

  const { name, site, group_id } = req.body as Partial<Companies>;

  if (name !== undefined && !String(name).trim()) return res.status(400).json({ message: 'Name cannot be empty' });
  if (site !== undefined && !String(site).trim()) return res.status(400).json({ message: 'Site cannot be empty' });

  existing.name = name !== undefined ? String(name).trim() : existing.name;
  existing.site = site !== undefined ? String(site).trim() : existing.site;
  if (group_id === null) {
    existing.group_id = null;
  } else if (group_id !== undefined) {
    const parsed = Number(group_id);
    if (!Number.isInteger(parsed) || parsed <= 0) return res.status(400).json({ message: 'group_id inválido' });
    existing.group_id = parsed;
  }

  const saved = await repo.save(existing);
  return res.json(saved);
});

function normalizeConfirmName(value: unknown): string {
  return String(value ?? '').trim();
}

async function ensureOwnerOrAdmin(req: Request, companyId: number): Promise<{ ok: true; userId: number } | { ok: false; status: number; message: string }> {
  const userId = await getAuthUserId(req);
  if (!userId) return { ok: false, status: 401, message: 'Não autenticado' };

  const user = await AppDataSource.getRepository(Users).findOne({ where: { id: userId } as any });
  if (!user) return { ok: false, status: 401, message: 'Não autenticado' };
  if (user.type === 'admin') return { ok: true, userId };

  const link = await AppDataSource.getRepository(CompanyUsers).findOne({ where: { user_id: userId, company_id: companyId } as any });
  if (!link) return { ok: false, status: 403, message: 'Sem acesso à empresa' };
  if (!(link as any).owner) return { ok: false, status: 403, message: 'Apenas o owner pode executar esta ação' };
  return { ok: true, userId };
}

companiesRouter.post('/me/danger/clear-data', async (req: Request, res: Response) => {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const perm = await ensureOwnerOrAdmin(req, companyId);
  if (!perm.ok) return res.status(perm.status).json({ message: perm.message });

  const companyRepo = AppDataSource.getRepository(Companies);
  const company = await companyRepo.findOne({ where: { id: companyId } as any });
  if (!company) return res.status(404).json({ message: 'Company not found' });

  const confirmName = normalizeConfirmName((req.body as any)?.confirm_name);
  const expectedName = normalizeConfirmName(company.name);
  if (!expectedName || confirmName !== expectedName) {
    return res.status(400).json({
      message: `Confirmação inválida. Digite exatamente o nome da empresa: "${expectedName}".`,
    });
  }

  try {
    const result = await AppDataSource.transaction(async (manager) => {
      // Ordem importante por FKs:
      // 1) order_items -> orders
      // 2) products (referenciados por order_items)
      // 3) customers (referenciados por orders)
      const orderItemsRes = await manager.getRepository(OrderItems).delete({ company_id: companyId } as any);
      const ordersRes = await manager.getRepository(Orders).delete({ company_id: companyId } as any);
      const productsRes = await manager.getRepository(Products).delete({ company_id: companyId } as any);
      const customersRes = await manager.getRepository(Customers).delete({ company_id: companyId } as any);

      return {
        deleted: {
          order_items: orderItemsRes.affected ?? 0,
          orders: ordersRes.affected ?? 0,
          products: productsRes.affected ?? 0,
          customers: customersRes.affected ?? 0,
        },
      };
    });

    return res.json({ ok: true, company_id: companyId, ...result });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao apagar dados' });
  }
});

companiesRouter.post('/me/danger/delete', async (req: Request, res: Response) => {
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const perm = await ensureOwnerOrAdmin(req, companyId);
  if (!perm.ok) return res.status(perm.status).json({ message: perm.message });

  const companyRepo = AppDataSource.getRepository(Companies);
  const company = await companyRepo.findOne({ where: { id: companyId } as any });
  if (!company) return res.status(404).json({ message: 'Company not found' });

  const confirmName = normalizeConfirmName((req.body as any)?.confirm_name);
  const expectedName = normalizeConfirmName(company.name);
  if (!expectedName || confirmName !== expectedName) {
    return res.status(400).json({
      message: `Confirmação inválida. Digite exatamente o nome da empresa: "${expectedName}".`,
    });
  }

  try {
    const result = await AppDataSource.transaction(async (manager) => {
      const groupId = company.group_id ?? null;

      const orderItemsRes = await manager.getRepository(OrderItems).delete({ company_id: companyId } as any);
      const ordersRes = await manager.getRepository(Orders).delete({ company_id: companyId } as any);
      const productsRes = await manager.getRepository(Products).delete({ company_id: companyId } as any);
      const customersRes = await manager.getRepository(Customers).delete({ company_id: companyId } as any);

      const companyUsersRes = await manager.getRepository(CompanyUsers).delete({ company_id: companyId } as any);
      const companyPlatformsRes = await manager.getRepository(CompanyPlatforms).delete({ company_id: companyId } as any);

      const companyRes = await manager.getRepository(Companies).delete({ id: companyId } as any);

      let groupDeleted = false;
      if (groupId) {
        const remaining = await manager.getRepository(Companies).count({ where: { group_id: groupId } as any });
        if (remaining === 0) {
          const groupRes = await manager.getRepository(Groups).delete({ id: groupId } as any);
          groupDeleted = (groupRes.affected ?? 0) > 0;
        }
      }

      return {
        deleted: {
          order_items: orderItemsRes.affected ?? 0,
          orders: ordersRes.affected ?? 0,
          products: productsRes.affected ?? 0,
          customers: customersRes.affected ?? 0,
          company_users: companyUsersRes.affected ?? 0,
          company_platforms: companyPlatformsRes.affected ?? 0,
          companies: companyRes.affected ?? 0,
        },
        group_deleted: groupDeleted,
        group_id: groupId,
      };
    });

    return res.json({ ok: true, company_id: companyId, ...result });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao deletar empresa' });
  }
});
