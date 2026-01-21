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
import fs from 'fs';
import path from 'path';

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

function safeTrimOrNull(v: unknown): string | null {
  if (v === undefined || v === null) return null;
  const s = String(v).trim();
  return s.length ? s : null;
}

function loadPromptFile(relFromSrc: string): string {
  const candidates = [
    // rodando via ts-node-dev (cwd = api/)
    path.join(process.cwd(), 'src', relFromSrc),
    // rodando via dist (cwd = api/, __dirname = dist/api)
    path.join(process.cwd(), 'dist', relFromSrc),
    // fallback relativo ao arquivo compilado
    path.join(__dirname, '..', relFromSrc),
  ];
  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) return fs.readFileSync(p, 'utf8');
    } catch {
      // ignore
    }
  }
  throw new Error(`Prompt não encontrado: ${relFromSrc}`);
}

function extractJsonFromText(raw: string): unknown {
  const s = String(raw || '').trim();
  if (!s) return null;
  // remove fences ```json ... ```
  const unfenced = s
    .replace(/^```json\s*/i, '')
    .replace(/^```\s*/i, '')
    .replace(/```$/i, '')
    .trim();
  try {
    return JSON.parse(unfenced);
  } catch {
    // tenta achar primeiro/último { } ou [ ]
    const firstObj = unfenced.indexOf('{');
    const lastObj = unfenced.lastIndexOf('}');
    const firstArr = unfenced.indexOf('[');
    const lastArr = unfenced.lastIndexOf(']');
    const objSlice = firstObj >= 0 && lastObj > firstObj ? unfenced.slice(firstObj, lastObj + 1) : null;
    const arrSlice = firstArr >= 0 && lastArr > firstArr ? unfenced.slice(firstArr, lastArr + 1) : null;
    const candidate = arrSlice ?? objSlice;
    if (!candidate) throw new Error('JSON inválido (não encontrado).');
    return JSON.parse(candidate);
  }
}

function extractOpenAIText(payload: any): string {
  if (!payload) return '';
  if (typeof payload.output_text === 'string') return payload.output_text;
  // responses API: output -> content -> text
  const out = payload.output;
  if (Array.isArray(out)) {
    const texts: string[] = [];
    for (const item of out) {
      const content = item?.content;
      if (Array.isArray(content)) {
        for (const c of content) {
          const t = c?.text;
          if (typeof t === 'string') texts.push(t);
        }
      }
    }
    if (texts.length) return texts.join('\n');
  }
  // fallback: message.content (chat completions style)
  const msg = payload.choices?.[0]?.message?.content;
  if (typeof msg === 'string') return msg;
  return '';
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
  const channelsFrom = String((req.query as any)?.channelsFrom ?? 'orders').trim(); // 'orders' | 'freight_quotes'

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
      channelsFrom === 'freight_quotes'
        ? `SELECT DISTINCT UPPER(TRIM(fq.channel)) AS value
           FROM freight_quotes fq
           JOIN companies c ON c.id = fq.company_id
           WHERE ${groupId ? 'c.group_id = $1' : 'fq.company_id = $1'}
             AND fq.channel IS NOT NULL
             AND TRIM(fq.channel) <> ''
           ORDER BY value ASC`
        : `SELECT DISTINCT COALESCE(o.marketplace_name, o.channel) AS value
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       WHERE ${condSql} AND COALESCE(o.marketplace_name, o.channel) IS NOT NULL
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
  // Evita cache/ETag atrapalhando resultados em filtros e em diferentes companies/usuários
  res.setHeader('Cache-Control', 'no-store');
  res.setHeader('Pragma', 'no-cache');
  res.setHeader('Expires', '0');
  res.setHeader('Vary', 'Authorization, X-Company-Id');

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
  // Para "Data do Processamento", preferimos filtrar por DIA (sem hora) para evitar confusão de fuso.
  // Aceita:
  // - processed-date=YYYY-MM-DD (novo/mais simples)
  // - processed-start / processed-end como YYYY-MM-DD (interpreta como range de datas)
  // - processed-start / processed-end como timestamptz (mantém compatibilidade)
  const processedDate = typeof q['processed-date'] === 'string' ? String(q['processed-date']).trim() : '';
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

  const processedDateIsYmd = isIsoYmd(processedDate);
  const processedStartIsYmd = isIsoYmd(processedStart);
  const processedEndIsYmd = isIsoYmd(processedEnd);

  if (processedDate && processedDateIsYmd) {
    // Filtra por "dia local" do processamento
    params.push(processedDate);
    where.push(`(l.processed_at AT TIME ZONE 'America/Sao_Paulo')::date = $${params.length}::date`);
  } else if ((processedStart && processedStartIsYmd) || (processedEnd && processedEndIsYmd)) {
    // Range por dia (sem hora), evitando timezone drift.
    if (processedStart && processedStartIsYmd) {
      params.push(processedStart);
      where.push(`(l.processed_at AT TIME ZONE 'America/Sao_Paulo')::date >= $${params.length}::date`);
    }
    if (processedEnd && processedEndIsYmd) {
      params.push(processedEnd);
      where.push(`(l.processed_at AT TIME ZONE 'America/Sao_Paulo')::date <= $${params.length}::date`);
    }
  } else {
    // Compat: timestamptz range (ex.: 2026-01-19T00:00:00Z)
    if (processedStart) {
      params.push(processedStart);
      where.push(`l.processed_at >= $${params.length}::timestamptz`);
    }
    if (processedEnd) {
      params.push(processedEnd);
      where.push(`l.processed_at <= $${params.length}::timestamptz`);
    }
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

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const param = groupId ?? companyId;

  const condSqlFq = groupId ? 'c.group_id = $1' : 'fq.company_id = $1';

  const statuses = toStringArray((req.query as any)?.status).map((s) => s.toLowerCase());
  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = Array.from(
    new Set([...toStringArray((req.query as any)?.sku), ...toStringArray((req.query as any)?.product)].map((s) => String(s).trim()).filter(Boolean)),
  );
  const companyIds = Array.from(
    new Set([...toStringArray((req.query as any)?.company_id), ...toStringArray((req.query as any)?.store)])
  )
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  // Drilldown de categoria (aplica somente no agregado byCategory deste endpoint)
  type CategoryDrillLevel = 'category' | 'subcategory' | 'final';
  const categoryLevelRaw = String((req.query as any)?.category_level ?? 'category')
    .trim()
    .toLowerCase();
  const categoryLevel: CategoryDrillLevel =
    categoryLevelRaw === 'subcategory' ? 'subcategory' : categoryLevelRaw === 'final' ? 'final' : 'category';
  const drillCategory = String((req.query as any)?.drill_category ?? '').trim();
  const drillSubcategory = String((req.query as any)?.drill_subcategory ?? '').trim();
  const categoryExpr =
    categoryLevel === 'category' ? 'p.category' : categoryLevel === 'subcategory' ? 'p.subcategory' : 'p.final_category';
  const categoryEmptyLabel =
    categoryLevel === 'category' ? '(sem categoria)' : categoryLevel === 'subcategory' ? '(sem subcategoria)' : '(sem categoria final)';
  const categoryIdExpr = `COALESCE(NULLIF(TRIM(${categoryExpr}), ''), '${categoryEmptyLabel}')`;
  const drillCategoryExpr = `COALESCE(NULLIF(TRIM(p.category), ''), '(sem categoria)')`;
  const drillSubcategoryExpr = `COALESCE(NULLIF(TRIM(p.subcategory), ''), '(sem subcategoria)')`;
  const drillCategoryParam = categoryLevel === 'category' ? null : drillCategory || null;
  const drillSubcategoryParam = categoryLevel === 'final' ? drillSubcategory || null : null;

  // regra: se filtrar por categoria/SKU, o faturamento deve vir do item (unit_price * quantity)
  const useItemsRevenue = Boolean(categories.length || skus.length);

  // regra de "faturamento" usada no resto do dashboard: total_amount - total_discount + shipping_amount
  const revenueExpr = useItemsRevenue
    ? `COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric`
    : `
      COALESCE(SUM(
        (COALESCE(o.total_amount, 0)::numeric
         - COALESCE(o.total_discount, 0)::numeric
         + COALESCE(o.shipping_amount, 0)::numeric)
      ), 0)::numeric
    `;

  const growthOffsets = [1, 7, 14, 21, 28];

  const commonWhere: string[] = [`${condSql}`, `o.order_date IS NOT NULL`];
  const commonParams: any[] = [param];

  if (groupId && companyIds.length) {
    commonParams.push(companyIds);
    commonWhere.push(`o.company_id = ANY($${commonParams.length}::int[])`);
  }
  if (channels.length) {
    commonParams.push(channels);
    commonWhere.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${commonParams.length}::text[])`);
  }
  if (states.length) {
    commonParams.push(states);
    commonWhere.push(`UPPER(TRIM(o.delivery_state)) = ANY($${commonParams.length}::text[])`);
  }
  if (cities.length) {
    commonParams.push(cities);
    commonWhere.push(`o.delivery_city = ANY($${commonParams.length}::text[])`);
  }
  if (statuses.length) {
    commonParams.push(statuses);
    commonWhere.push(`LOWER(TRIM(COALESCE(o.current_status, ''))) = ANY($${commonParams.length}::text[])`);
  }

  const baseJoinSql = useItemsRevenue
    ? `JOIN order_items i ON i.order_id = o.id
       JOIN products p ON p.id = i.product_id`
    : ``;

  const filterSqlExtra = useItemsRevenue
    ? `
      AND (CASE WHEN $${commonParams.length + 1}::text[] IS NULL OR array_length($${commonParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 1}::text[]) END)
      AND (CASE WHEN $${commonParams.length + 2}::text[] IS NULL OR array_length($${commonParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 2}::text[]) END)
    `
    : ``;
  const filterParamsExtra = useItemsRevenue ? [categories.length ? categories : null, skus.length ? skus : null] : [];

  const [
    todayRows,
    periodRows,
    dailyRows,
    ordersCountRows,
    itemsSoldRows,
    quotesCountRows,
    uniqueCustomersRows,
    byMarketplaceRows,
    byProductRows,
    byBrandRows,
    byCategoryAggRows,
    ...growthRowsList
  ] = await Promise.all([
    AppDataSource.query(
      `SELECT ${revenueExpr} AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       ${baseJoinSql}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date = ((now() AT TIME ZONE 'America/Sao_Paulo')::date)
       ${filterSqlExtra}`,
      [...commonParams, ...filterParamsExtra],
    ),
    AppDataSource.query(
      `SELECT ${revenueExpr} AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       ${baseJoinSql}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
       ${filterSqlExtra}`,
      [...commonParams, start, end, ...filterParamsExtra],
    ),
    AppDataSource.query(
      `SELECT
         o.order_date::date AS day_date,
         to_char(o.order_date::date, 'YYYY-MM-DD') AS ymd,
         to_char(o.order_date::date, 'DD/MM/YYYY') AS day,
         ${revenueExpr} AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       ${baseJoinSql}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
       ${filterSqlExtra}
       GROUP BY 1, 2, 3
       ORDER BY 1 ASC`,
      [...commonParams, start, end, ...filterParamsExtra],
    ),
    AppDataSource.query(
      `SELECT COUNT(DISTINCT o.id) AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       ${baseJoinSql}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
       ${filterSqlExtra}`,
      [...commonParams, start, end, ...filterParamsExtra],
    ),
    AppDataSource.query(
      `SELECT COALESCE(SUM(i.quantity::int), 0)::int AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       JOIN order_items i ON i.order_id = o.id
       ${useItemsRevenue ? `JOIN products p ON p.id = i.product_id` : ``}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
         ${
           useItemsRevenue
             ? `
           AND (CASE WHEN $${commonParams.length + 3}::text[] IS NULL OR array_length($${commonParams.length + 3}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 3}::text[]) END)
           AND (CASE WHEN $${commonParams.length + 4}::text[] IS NULL OR array_length($${commonParams.length + 4}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 4}::text[]) END)
         `
             : ``
         }
      `,
      [...commonParams, start, end, ...(useItemsRevenue ? [categories.length ? categories : null, skus.length ? skus : null] : [])],
    ),
    AppDataSource.query(
      `SELECT COUNT(*) AS total
       FROM freight_quotes fq
       JOIN companies c ON c.id = fq.company_id
       WHERE ${condSqlFq}
         AND fq.quoted_at IS NOT NULL
         AND fq.quoted_at::date BETWEEN $2::date AND $3::date
         ${groupId && companyIds.length ? `AND fq.company_id = ANY($4::int[])` : ``}
         ${channels.length ? `AND UPPER(TRIM(fq.channel)) = ANY($${groupId && companyIds.length ? 5 : 4}::text[])` : ``}
      `,
      [param, start, end, ...(groupId && companyIds.length ? [companyIds] : []), ...(channels.length ? [channels.map((c) => String(c).toUpperCase().trim()).filter(Boolean)] : [])],
    ),
    AppDataSource.query(
      `SELECT COUNT(DISTINCT o.customer_id) AS total
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       ${baseJoinSql}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
         AND o.customer_id IS NOT NULL`,
      [...commonParams, start, end, ...filterParamsExtra],
    ),
    AppDataSource.query(
      `SELECT
         COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), 'Sem canal') AS marketplace,
         COUNT(DISTINCT o.id)::int AS orders_count,
         ${revenueExpr} AS revenue
       FROM orders o
       JOIN companies c ON c.id = o.company_id
       ${baseJoinSql}
       WHERE ${commonWhere.join(' AND ')}
         AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
       ${filterSqlExtra}
       GROUP BY 1
       ORDER BY revenue DESC, marketplace ASC`,
      [...commonParams, start, end, ...filterParamsExtra],
    ),
    // Lista de SKUs vendidos (sempre via itens para atribuir faturamento ao produto)
    AppDataSource.query(
      `
      SELECT
        p.id AS product_id,
        p.sku AS sku,
        MAX(p.name) AS name,
        COUNT(DISTINCT o.id)::int AS orders_count,
        COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue,
        COALESCE(SUM(i.quantity::int), 0)::int AS qty
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      JOIN order_items i ON i.order_id = o.id
      JOIN products p ON p.id = i.product_id
      WHERE ${commonWhere.join(' AND ')}
        AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
        AND (CASE WHEN $${commonParams.length + 3}::text[] IS NULL OR array_length($${commonParams.length + 3}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 3}::text[]) END)
        AND (CASE WHEN $${commonParams.length + 4}::text[] IS NULL OR array_length($${commonParams.length + 4}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 4}::text[]) END)
      GROUP BY 1, 2
      ORDER BY revenue DESC NULLS LAST
      LIMIT 100
      `,
      [...commonParams, start, end, categories.length ? categories : null, skus.length ? skus : null],
    ),
    // Breakdown por marca/categoria: sempre via itens (para atribuir faturamento corretamente)
    AppDataSource.query(
      `
      SELECT
        COALESCE(NULLIF(TRIM(p.brand), ''), '(sem marca)') AS id,
        COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      JOIN order_items i ON i.order_id = o.id
      JOIN products p ON p.id = i.product_id
      WHERE ${commonWhere.join(' AND ')}
        AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
        AND (CASE WHEN $${commonParams.length + 3}::text[] IS NULL OR array_length($${commonParams.length + 3}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 3}::text[]) END)
        AND (CASE WHEN $${commonParams.length + 4}::text[] IS NULL OR array_length($${commonParams.length + 4}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 4}::text[]) END)
      GROUP BY 1
      ORDER BY revenue DESC NULLS LAST, id ASC
      LIMIT 12
      `,
      [...commonParams, start, end, categories.length ? categories : null, skus.length ? skus : null],
    ),
    AppDataSource.query(
      `
      SELECT
        ${categoryIdExpr} AS id,
        COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      JOIN order_items i ON i.order_id = o.id
      JOIN products p ON p.id = i.product_id
      WHERE ${commonWhere.join(' AND ')}
        AND o.order_date::date BETWEEN $${commonParams.length + 1}::date AND $${commonParams.length + 2}::date
        AND (CASE WHEN $${commonParams.length + 3}::text[] IS NULL OR array_length($${commonParams.length + 3}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 3}::text[]) END)
        AND (CASE WHEN $${commonParams.length + 4}::text[] IS NULL OR array_length($${commonParams.length + 4}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 4}::text[]) END)
        AND (CASE WHEN $${commonParams.length + 5}::text IS NULL OR $${commonParams.length + 5}::text = '' THEN TRUE ELSE ${drillCategoryExpr} = $${commonParams.length + 5}::text END)
        AND (CASE WHEN $${commonParams.length + 6}::text IS NULL OR $${commonParams.length + 6}::text = '' THEN TRUE ELSE ${drillSubcategoryExpr} = $${commonParams.length + 6}::text END)
      GROUP BY 1
      ORDER BY revenue DESC NULLS LAST, id ASC
      LIMIT 12
      `,
      [...commonParams, start, end, categories.length ? categories : null, skus.length ? skus : null, drillCategoryParam, drillSubcategoryParam],
    ),
    ...growthOffsets.map((offsetDays) =>
      AppDataSource.query(
        `SELECT ${revenueExpr} AS total
         FROM orders o
         JOIN companies c ON c.id = o.company_id
         ${baseJoinSql}
         WHERE ${commonWhere.join(' AND ')}
           AND o.order_date::date BETWEEN ($${commonParams.length + 1}::date - $${commonParams.length + 3}::int) AND ($${commonParams.length + 2}::date - $${commonParams.length + 3}::int)
         ${filterSqlExtra}`,
        [...commonParams, start, end, offsetDays, ...filterParamsExtra],
      ),
    ),
  ]);

  const todayTotal = Number((todayRows?.[0] as any)?.total ?? 0) || 0;
  const periodTotal = Number((periodRows?.[0] as any)?.total ?? 0) || 0;
  const ordersCount = Number((ordersCountRows?.[0] as any)?.total ?? 0) || 0;
  const itemsSold = Number((itemsSoldRows?.[0] as any)?.total ?? 0) || 0;
  const quotesCount = Number((quotesCountRows?.[0] as any)?.total ?? 0) || 0;
  const uniqueCustomers = Number((uniqueCustomersRows?.[0] as any)?.total ?? 0) || 0;
  const conversionRate = quotesCount > 0 ? ordersCount / quotesCount : 0;
  const daily = (dailyRows || []).map((r: any) => ({
    ymd: String(r.ymd || ''), // YYYY-MM-DD
    date: String(r.day), // DD/MM/YYYY (para o gráfico)
    total: Number(r.total ?? 0) || 0,
  }));

  const avgTicket = ordersCount > 0 ? periodTotal / ordersCount : 0;

  const byMarketplace = (byMarketplaceRows || []).map((r: any) => {
    const revenue = Number(r.revenue ?? 0) || 0;
    const ordersCount = Number(r.orders_count ?? 0) || 0;
    return {
      marketplace: String(r.marketplace || 'Sem canal'),
      revenue,
      ordersCount,
      avgTicket: ordersCount > 0 ? revenue / ordersCount : 0,
    };
  });

  const byBrand = (byBrandRows || []).map((r: any) => ({ id: String(r.id), revenue: Number(r.revenue ?? 0) || 0 }));
  const byCategoryAgg = (byCategoryAggRows || []).map((r: any) => ({ id: String(r.id), revenue: Number(r.revenue ?? 0) || 0 }));
  const byProductTable = (byProductRows || []).map((r: any) => {
    const revenue = Number(r.revenue ?? 0) || 0;
    const ordersCount = Number(r.orders_count ?? 0) || 0;
    return {
      productId: Number(r.product_id ?? 0) || null,
      sku: String(r.sku ?? ''),
      name: r.name ?? null,
      qty: Number(r.qty ?? 0) || 0,
      revenue,
      ordersCount,
      avgTicket: ordersCount > 0 ? revenue / ordersCount : 0,
    };
  });

  const growth = growthOffsets.map((offsetDays, idx) => {
    const prev = Number((growthRowsList?.[idx]?.[0] as any)?.total ?? 0) || 0;
    const value = prev > 0 ? (periodTotal - prev) / prev : null;
    return { offsetDays, label: `D-${offsetDays}`, value, previous: prev, current: periodTotal };
  });

  return res.json({
    companyId,
    groupId,
    start,
    end,
    today: todayTotal,
    period: periodTotal,
    ordersCount,
    itemsSold,
    quotesCount,
    conversionRate,
    uniqueCustomers,
    avgTicket,
    growth,
    byMarketplace,
    byProductTable,
    byBrand,
    byCategory: byCategoryAgg,
    daily,
  });
});

companiesRouter.get('/me/dashboard/live/overview', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const { companyId, groupId } = filter;
  const param = groupId ?? companyId;
  const condOrders = groupId ? 'c.group_id = $1' : 'o.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = toStringArray((req.query as any)?.sku);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  // Drilldown de categoria (aplica somente nos agregados de categoria)
  type CategoryDrillLevel = 'category' | 'subcategory' | 'final';
  const categoryLevelRaw = String((req.query as any)?.category_level ?? 'category')
    .trim()
    .toLowerCase();
  const categoryLevel: CategoryDrillLevel =
    categoryLevelRaw === 'subcategory' ? 'subcategory' : categoryLevelRaw === 'final' ? 'final' : 'category';
  const drillCategory = String((req.query as any)?.drill_category ?? '').trim();
  const drillSubcategory = String((req.query as any)?.drill_subcategory ?? '').trim();
  const categoryExpr =
    categoryLevel === 'category' ? 'p.category' : categoryLevel === 'subcategory' ? 'p.subcategory' : 'p.final_category';
  const categoryEmptyLabel =
    categoryLevel === 'category' ? '(sem categoria)' : categoryLevel === 'subcategory' ? '(sem subcategoria)' : '(sem categoria final)';
  const categoryIdExpr = `COALESCE(NULLIF(TRIM(${categoryExpr}), ''), '${categoryEmptyLabel}')`;
  const drillCategoryExpr = `COALESCE(NULLIF(TRIM(p.category), ''), '(sem categoria)')`;
  const drillSubcategoryExpr = `COALESCE(NULLIF(TRIM(p.subcategory), ''), '(sem subcategoria)')`;
  const drillCategoryParam = categoryLevel === 'category' ? null : drillCategory || null;
  const drillSubcategoryParam = categoryLevel === 'final' ? drillSubcategory || null : null;

  // regra: se filtrar por categoria/SKU, o faturamento deve vir do item (unit_price * quantity)
  const useItemsRevenue = Boolean(categories.length || skus.length);

  try {
    const baseDayRaw = String((req.query as any)?.day ?? '').trim();
    const baseDay = baseDayRaw ? baseDayRaw : null;
    if (baseDay && !isIsoYmd(baseDay)) {
      return res.status(400).json({ message: 'Parâmetro inválido: day deve estar em YYYY-MM-DD.' });
    }

    // IMPORTANTE:
    // - Retorna tudo como TEXT (YYYY-MM-DD) para evitar "GMT-0300" vindo do parser do driver.
    // - Usa "America/Sao_Paulo" para "agora" e datas de referência, evitando divergência quando o Postgres está em UTC.
    const metaRows = await AppDataSource.query(
      `
      WITH x AS (
        SELECT
          (now() AT TIME ZONE 'America/Sao_Paulo')::date AS actual_today,
          COALESCE($1::date, (now() AT TIME ZONE 'America/Sao_Paulo')::date) AS base_day
      )
      SELECT
        to_char(actual_today, 'YYYY-MM-DD') AS actual_today,
        to_char(base_day, 'YYYY-MM-DD') AS today,
        to_char((base_day - 1), 'YYYY-MM-DD') AS yesterday,
        to_char((base_day - 7), 'YYYY-MM-DD') AS d7,
        to_char((base_day - 14), 'YYYY-MM-DD') AS d14,
        to_char((base_day - 21), 'YYYY-MM-DD') AS d21,
        to_char((base_day - 28), 'YYYY-MM-DD') AS d28,
        to_char((base_day - 8), 'YYYY-MM-DD') AS d8,
        (base_day = actual_today) AS is_live,
        CASE WHEN base_day = actual_today THEN EXTRACT(HOUR FROM (now() AT TIME ZONE 'America/Sao_Paulo'))::int ELSE 23 END AS current_hour
      FROM x
      `,
      [baseDay],
    );
    const meta = metaRows?.[0] || {};
    const actualToday = String(meta.actual_today || '').trim();
    const today = String(meta.today || '').trim();
    const yesterday = String(meta.yesterday || '').trim();
    const d7 = String(meta.d7 || '').trim();
    const d14 = String(meta.d14 || '').trim();
    const d21 = String(meta.d21 || '').trim();
    const d28 = String(meta.d28 || '').trim();
    const d8 = String(meta.d8 || '').trim();
    const currentHour = Number(meta.current_hour ?? 0) || 0;
    const isLive = Boolean(meta.is_live);

    if (baseDay && actualToday && baseDay > actualToday) {
      return res.status(400).json({ message: `Parâmetro inválido: day não pode ser maior que hoje (${actualToday}).` });
    }

    const days = [today, yesterday, d7, d14, d21, d28, d8];

    const commonParams: any[] = [param, days];
    const where: string[] = [
      `${condOrders}`,
      `o.order_date IS NOT NULL`,
      `o.order_date::date = ANY($2::date[])`,
    ];
    if (groupId && companyIds.length) {
      commonParams.push(companyIds);
      where.push(`o.company_id = ANY($${commonParams.length}::int[])`);
    }
    if (channels.length) {
      commonParams.push(channels);
      where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${commonParams.length}::text[])`);
    }
    if (states.length) {
      commonParams.push(states);
      where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${commonParams.length}::text[])`);
    }
    if (cities.length) {
      commonParams.push(cities);
      where.push(`o.delivery_city = ANY($${commonParams.length}::text[])`);
    }

    // 1) Receita por hora (cumulativa será montada no front)
    const hourlyRows = await AppDataSource.query(
      useItemsRevenue
        ? `
          SELECT
            to_char(o.order_date::date, 'YYYY-MM-DD') AS day,
            EXTRACT(HOUR FROM o.order_date)::int AS hour,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${where.join(' AND ')}
            AND (CASE WHEN $${commonParams.length + 1}::text[] IS NULL OR array_length($${commonParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 1}::text[]) END)
            AND (CASE WHEN $${commonParams.length + 2}::text[] IS NULL OR array_length($${commonParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 2}::text[]) END)
          GROUP BY o.order_date::date, 2
          ORDER BY o.order_date::date ASC, 2 ASC
        `
        : `
          SELECT
            to_char(o.order_date::date, 'YYYY-MM-DD') AS day,
            EXTRACT(HOUR FROM o.order_date)::int AS hour,
            COALESCE(SUM(
              COALESCE(o.total_amount::numeric, 0)
              - COALESCE(o.total_discount::numeric, 0)
              + COALESCE(o.shipping_amount::numeric, 0)
            ), 0)::numeric AS revenue
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          WHERE ${where.join(' AND ')}
          GROUP BY o.order_date::date, 2
          ORDER BY o.order_date::date ASC, 2 ASC
        `,
      useItemsRevenue ? [...commonParams, categories.length ? categories : null, skus.length ? skus : null] : commonParams,
    );

    // 2) KPIs por dia (para escolher "Ontem/D-7/etc" no front)
    const kpiRows = await AppDataSource.query(
      useItemsRevenue
        ? `
          SELECT
            to_char(o.order_date::date, 'YYYY-MM-DD') AS day,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue,
            COUNT(DISTINCT o.id)::int AS orders,
            COUNT(DISTINCT o.customer_id)::int AS customers,
            COALESCE(SUM(i.quantity::int), 0)::int AS items_sold,
            COUNT(DISTINCT p.sku)::int AS skus_count
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${where.join(' AND ')}
            AND (CASE WHEN $${commonParams.length + 1}::text[] IS NULL OR array_length($${commonParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${commonParams.length + 1}::text[]) END)
            AND (CASE WHEN $${commonParams.length + 2}::text[] IS NULL OR array_length($${commonParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${commonParams.length + 2}::text[]) END)
          GROUP BY o.order_date::date
          ORDER BY o.order_date::date ASC
        `
        : `
          SELECT
            to_char(o.order_date::date, 'YYYY-MM-DD') AS day,
            COALESCE(SUM(
              COALESCE(o.total_amount::numeric, 0)
              - COALESCE(o.total_discount::numeric, 0)
              + COALESCE(o.shipping_amount::numeric, 0)
            ), 0)::numeric AS revenue,
            COUNT(DISTINCT o.id)::int AS orders,
            COUNT(DISTINCT o.customer_id)::int AS customers,
            0::int AS items_sold,
            0::int AS skus_count
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          WHERE ${where.join(' AND ')}
          GROUP BY o.order_date::date
          ORDER BY o.order_date::date ASC
        `,
      useItemsRevenue ? [...commonParams, categories.length ? categories : null, skus.length ? skus : null] : commonParams,
    );

    // 3) Itens vendidos por dia (mesmo quando revenue vem de orders)
    const itemsSoldRows = useItemsRevenue
      ? []
      : await AppDataSource.query(
          `
          SELECT
            to_char(o.order_date::date, 'YYYY-MM-DD') AS day,
            COALESCE(SUM(i.quantity::int), 0)::int AS items_sold
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          WHERE ${where.join(' AND ')}
          GROUP BY o.order_date::date
          ORDER BY o.order_date::date ASC
        `,
          commonParams,
        );

    // 3b) SKUs distintos por dia (mesmo quando revenue vem de orders)
    const skusCountRows = useItemsRevenue
      ? []
      : await AppDataSource.query(
          `
          SELECT
            to_char(o.order_date::date, 'YYYY-MM-DD') AS day,
            COUNT(DISTINCT p.sku)::int AS skus_count
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${where.join(' AND ')}
          GROUP BY o.order_date::date
          ORDER BY o.order_date::date ASC
        `,
          commonParams,
        );

    // 4) Breakdown (hoje)
    const dayParamsBase: any[] = [param, today];
    const whereToday: string[] = [
      `${condOrders}`,
      `o.order_date IS NOT NULL`,
      `o.order_date::date = $2::date`,
    ];
    if (groupId && companyIds.length) {
      dayParamsBase.push(companyIds);
      whereToday.push(`o.company_id = ANY($${dayParamsBase.length}::int[])`);
    }
    if (channels.length) {
      dayParamsBase.push(channels);
      whereToday.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${dayParamsBase.length}::text[])`);
    }
    if (states.length) {
      dayParamsBase.push(states);
      whereToday.push(`UPPER(TRIM(o.delivery_state)) = ANY($${dayParamsBase.length}::text[])`);
    }
    if (cities.length) {
      dayParamsBase.push(cities);
      whereToday.push(`o.delivery_city = ANY($${dayParamsBase.length}::text[])`);
    }

    const byMarketplace = await AppDataSource.query(
      useItemsRevenue
        ? `
          SELECT
            COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)') AS id,
            COUNT(DISTINCT o.id)::int AS orders_count,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS value
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${whereToday.join(' AND ')}
            AND (CASE WHEN $${dayParamsBase.length + 1}::text[] IS NULL OR array_length($${dayParamsBase.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParamsBase.length + 1}::text[]) END)
            AND (CASE WHEN $${dayParamsBase.length + 2}::text[] IS NULL OR array_length($${dayParamsBase.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParamsBase.length + 2}::text[]) END)
          GROUP BY 1
          ORDER BY value DESC, id ASC
        `
        : `
          SELECT
            COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)') AS id,
            COUNT(*)::int AS orders_count,
            COALESCE(SUM(
              COALESCE(o.total_amount::numeric, 0)
              - COALESCE(o.total_discount::numeric, 0)
              + COALESCE(o.shipping_amount::numeric, 0)
            ), 0)::numeric AS value
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          WHERE ${whereToday.join(' AND ')}
          GROUP BY 1
          ORDER BY value DESC, id ASC
        `,
      useItemsRevenue ? [...dayParamsBase, categories.length ? categories : null, skus.length ? skus : null] : dayParamsBase,
    );

    const toMarketplaceTable = (rows: any[]) =>
      (rows || []).map((r: any) => {
        const revenue = Number(r.value ?? 0) || 0;
        const ordersCount = Number(r.orders_count ?? 0) || 0;
        return { id: String(r.id), revenue, ordersCount, avgTicket: ordersCount > 0 ? revenue / ordersCount : 0 };
      });

    const fetchByMarketplaceForDay = async (dayYmd: string) => {
      const dayParams: any[] = [param, dayYmd];
      const whereDay: string[] = [`${condOrders}`, `o.order_date IS NOT NULL`, `o.order_date::date = $2::date`];
      if (groupId && companyIds.length) {
        dayParams.push(companyIds);
        whereDay.push(`o.company_id = ANY($${dayParams.length}::int[])`);
      }
      if (channels.length) {
        dayParams.push(channels);
        whereDay.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${dayParams.length}::text[])`);
      }
      if (states.length) {
        dayParams.push(states);
        whereDay.push(`UPPER(TRIM(o.delivery_state)) = ANY($${dayParams.length}::text[])`);
      }
      if (cities.length) {
        dayParams.push(cities);
        whereDay.push(`o.delivery_city = ANY($${dayParams.length}::text[])`);
      }

      const rows = await AppDataSource.query(
        useItemsRevenue
          ? `
            SELECT
              COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)') AS id,
              COUNT(DISTINCT o.id)::int AS orders_count,
              COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS value
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            JOIN order_items i ON i.order_id = o.id
            JOIN products p ON p.id = i.product_id
            WHERE ${whereDay.join(' AND ')}
              AND (CASE WHEN $${dayParams.length + 1}::text[] IS NULL OR array_length($${dayParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParams.length + 1}::text[]) END)
              AND (CASE WHEN $${dayParams.length + 2}::text[] IS NULL OR array_length($${dayParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParams.length + 2}::text[]) END)
            GROUP BY 1
            ORDER BY value DESC, id ASC
          `
          : `
            SELECT
              COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)') AS id,
              COUNT(*)::int AS orders_count,
              COALESCE(SUM(
                COALESCE(o.total_amount::numeric, 0)
                - COALESCE(o.total_discount::numeric, 0)
                + COALESCE(o.shipping_amount::numeric, 0)
              ), 0)::numeric AS value
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            WHERE ${whereDay.join(' AND ')}
            GROUP BY 1
            ORDER BY value DESC, id ASC
          `,
        useItemsRevenue ? [...dayParams, categories.length ? categories : null, skus.length ? skus : null] : dayParams,
      );
      return toMarketplaceTable(rows);
    };

    const [bmYesterday, bmD7, bmD14, bmD21, bmD28] = await Promise.all([
      fetchByMarketplaceForDay(yesterday),
      fetchByMarketplaceForDay(d7),
      fetchByMarketplaceForDay(d14),
      fetchByMarketplaceForDay(d21),
      fetchByMarketplaceForDay(d28),
    ]);

    const byState = await AppDataSource.query(
      useItemsRevenue
        ? `
          SELECT
            COALESCE(NULLIF(TRIM(UPPER(o.delivery_state)), ''), '(sem UF)') AS id,
            COUNT(DISTINCT o.id)::int AS orders_count,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS value
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${whereToday.join(' AND ')}
            AND (CASE WHEN $${dayParamsBase.length + 1}::text[] IS NULL OR array_length($${dayParamsBase.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParamsBase.length + 1}::text[]) END)
            AND (CASE WHEN $${dayParamsBase.length + 2}::text[] IS NULL OR array_length($${dayParamsBase.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParamsBase.length + 2}::text[]) END)
          GROUP BY 1
          ORDER BY value DESC, id ASC
        `
        : `
          SELECT
            COALESCE(NULLIF(TRIM(UPPER(o.delivery_state)), ''), '(sem UF)') AS id,
            COUNT(*)::int AS orders_count,
            COALESCE(SUM(
              COALESCE(o.total_amount::numeric, 0)
              - COALESCE(o.total_discount::numeric, 0)
              + COALESCE(o.shipping_amount::numeric, 0)
            ), 0)::numeric AS value
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          WHERE ${whereToday.join(' AND ')}
          GROUP BY 1
          ORDER BY value DESC, id ASC
        `,
      useItemsRevenue ? [...dayParamsBase, categories.length ? categories : null, skus.length ? skus : null] : dayParamsBase,
    );

    const byCategory = await AppDataSource.query(
      `
      SELECT
        ${categoryIdExpr} AS id,
        COUNT(DISTINCT o.id)::int AS orders_count,
        COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS value
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      JOIN order_items i ON i.order_id = o.id
      JOIN products p ON p.id = i.product_id
      WHERE ${whereToday.join(' AND ')}
        AND (CASE WHEN $${dayParamsBase.length + 1}::text[] IS NULL OR array_length($${dayParamsBase.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParamsBase.length + 1}::text[]) END)
        AND (CASE WHEN $${dayParamsBase.length + 2}::text[] IS NULL OR array_length($${dayParamsBase.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParamsBase.length + 2}::text[]) END)
        AND (CASE WHEN $${dayParamsBase.length + 3}::text IS NULL OR $${dayParamsBase.length + 3}::text = '' THEN TRUE ELSE ${drillCategoryExpr} = $${dayParamsBase.length + 3}::text END)
        AND (CASE WHEN $${dayParamsBase.length + 4}::text IS NULL OR $${dayParamsBase.length + 4}::text = '' THEN TRUE ELSE ${drillSubcategoryExpr} = $${dayParamsBase.length + 4}::text END)
      GROUP BY 1
      ORDER BY value DESC, id ASC
      `,
      [...dayParamsBase, categories.length ? categories : null, skus.length ? skus : null, drillCategoryParam, drillSubcategoryParam],
    );

    const toStateTable = (rows: any[]) =>
      (rows || []).map((r: any) => {
        const revenue = Number(r.value ?? 0) || 0;
        const ordersCount = Number(r.orders_count ?? 0) || 0;
        return { id: String(r.id), revenue, ordersCount, avgTicket: ordersCount > 0 ? revenue / ordersCount : 0 };
      });

    const toCategoryTable = (rows: any[]) =>
      (rows || []).map((r: any) => {
        const revenue = Number(r.value ?? 0) || 0;
        const ordersCount = Number(r.orders_count ?? 0) || 0;
        return { id: String(r.id), revenue, ordersCount, avgTicket: ordersCount > 0 ? revenue / ordersCount : 0 };
      });

    const fetchByStateForDay = async (dayYmd: string) => {
      const dayParams: any[] = [param, dayYmd];
      const whereDay: string[] = [`${condOrders}`, `o.order_date IS NOT NULL`, `o.order_date::date = $2::date`];
      if (groupId && companyIds.length) {
        dayParams.push(companyIds);
        whereDay.push(`o.company_id = ANY($${dayParams.length}::int[])`);
      }
      if (channels.length) {
        dayParams.push(channels);
        whereDay.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${dayParams.length}::text[])`);
      }
      if (states.length) {
        dayParams.push(states);
        whereDay.push(`UPPER(TRIM(o.delivery_state)) = ANY($${dayParams.length}::text[])`);
      }
      if (cities.length) {
        dayParams.push(cities);
        whereDay.push(`o.delivery_city = ANY($${dayParams.length}::text[])`);
      }

      const rows = await AppDataSource.query(
        useItemsRevenue
          ? `
            SELECT
              COALESCE(NULLIF(TRIM(UPPER(o.delivery_state)), ''), '(sem UF)') AS id,
              COUNT(DISTINCT o.id)::int AS orders_count,
              COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS value
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            JOIN order_items i ON i.order_id = o.id
            JOIN products p ON p.id = i.product_id
            WHERE ${whereDay.join(' AND ')}
              AND (CASE WHEN $${dayParams.length + 1}::text[] IS NULL OR array_length($${dayParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParams.length + 1}::text[]) END)
              AND (CASE WHEN $${dayParams.length + 2}::text[] IS NULL OR array_length($${dayParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParams.length + 2}::text[]) END)
            GROUP BY 1
            ORDER BY value DESC, id ASC
          `
          : `
            SELECT
              COALESCE(NULLIF(TRIM(UPPER(o.delivery_state)), ''), '(sem UF)') AS id,
              COUNT(*)::int AS orders_count,
              COALESCE(SUM(
                COALESCE(o.total_amount::numeric, 0)
                - COALESCE(o.total_discount::numeric, 0)
                + COALESCE(o.shipping_amount::numeric, 0)
              ), 0)::numeric AS value
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            WHERE ${whereDay.join(' AND ')}
            GROUP BY 1
            ORDER BY value DESC, id ASC
          `,
        useItemsRevenue ? [...dayParams, categories.length ? categories : null, skus.length ? skus : null] : dayParams,
      );
      return toStateTable(rows);
    };

    const fetchByCategoryForDay = async (dayYmd: string) => {
      const dayParams: any[] = [param, dayYmd];
      const whereDay: string[] = [`${condOrders}`, `o.order_date IS NOT NULL`, `o.order_date::date = $2::date`];
      if (groupId && companyIds.length) {
        dayParams.push(companyIds);
        whereDay.push(`o.company_id = ANY($${dayParams.length}::int[])`);
      }
      if (channels.length) {
        dayParams.push(channels);
        whereDay.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${dayParams.length}::text[])`);
      }
      if (states.length) {
        dayParams.push(states);
        whereDay.push(`UPPER(TRIM(o.delivery_state)) = ANY($${dayParams.length}::text[])`);
      }
      if (cities.length) {
        dayParams.push(cities);
        whereDay.push(`o.delivery_city = ANY($${dayParams.length}::text[])`);
      }

      const rows = await AppDataSource.query(
        `
          SELECT
            ${categoryIdExpr} AS id,
            COUNT(DISTINCT o.id)::int AS orders_count,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS value
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${whereDay.join(' AND ')}
            AND (CASE WHEN $${dayParams.length + 1}::text[] IS NULL OR array_length($${dayParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParams.length + 1}::text[]) END)
            AND (CASE WHEN $${dayParams.length + 2}::text[] IS NULL OR array_length($${dayParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParams.length + 2}::text[]) END)
            AND (CASE WHEN $${dayParams.length + 3}::text IS NULL OR $${dayParams.length + 3}::text = '' THEN TRUE ELSE ${drillCategoryExpr} = $${dayParams.length + 3}::text END)
            AND (CASE WHEN $${dayParams.length + 4}::text IS NULL OR $${dayParams.length + 4}::text = '' THEN TRUE ELSE ${drillSubcategoryExpr} = $${dayParams.length + 4}::text END)
          GROUP BY 1
          ORDER BY value DESC, id ASC
        `,
        [...dayParams, categories.length ? categories : null, skus.length ? skus : null, drillCategoryParam, drillSubcategoryParam],
      );
      return toCategoryTable(rows);
    };

    const fetchByProductForDay = async (dayYmd: string) => {
      const dayParams: any[] = [param, dayYmd];
      const whereDay: string[] = [`${condOrders}`, `o.order_date IS NOT NULL`, `o.order_date::date = $2::date`];
      if (groupId && companyIds.length) {
        dayParams.push(companyIds);
        whereDay.push(`o.company_id = ANY($${dayParams.length}::int[])`);
      }
      if (channels.length) {
        dayParams.push(channels);
        whereDay.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${dayParams.length}::text[])`);
      }
      if (states.length) {
        dayParams.push(states);
        whereDay.push(`UPPER(TRIM(o.delivery_state)) = ANY($${dayParams.length}::text[])`);
      }
      if (cities.length) {
        dayParams.push(cities);
        whereDay.push(`o.delivery_city = ANY($${dayParams.length}::text[])`);
      }

      const rows = await AppDataSource.query(
        `
          SELECT
            MAX(p.id)::int AS product_id,
            p.sku AS sku,
            MAX(p.name) AS name,
            COUNT(DISTINCT o.id)::int AS orders_count,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${whereDay.join(' AND ')}
            AND (CASE WHEN $${dayParams.length + 1}::text[] IS NULL OR array_length($${dayParams.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParams.length + 1}::text[]) END)
            AND (CASE WHEN $${dayParams.length + 2}::text[] IS NULL OR array_length($${dayParams.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParams.length + 2}::text[]) END)
          GROUP BY p.sku
          ORDER BY revenue DESC NULLS LAST, sku ASC
        `,
        [...dayParams, categories.length ? categories : null, skus.length ? skus : null],
      );

      return (rows || []).map((r: any) => {
        const revenue = Number(r.revenue ?? 0) || 0;
        const ordersCount = Number(r.orders_count ?? 0) || 0;
        return {
          productId: Number(r.product_id ?? 0) || null,
          sku: String(r.sku ?? ''),
          name: r.name ?? null,
          revenue,
          ordersCount,
          avgTicket: ordersCount > 0 ? revenue / ordersCount : 0,
        };
      });
    };

    const [prodToday, prodYesterday, stateYesterday, stateD7, stateD14, stateD21, stateD28, catYesterday, catD7, catD14, catD21, catD28] = await Promise.all([
      fetchByProductForDay(today),
      fetchByProductForDay(yesterday),
      fetchByStateForDay(yesterday),
      fetchByStateForDay(d7),
      fetchByStateForDay(d14),
      fetchByStateForDay(d21),
      fetchByStateForDay(d28),
      fetchByCategoryForDay(yesterday),
      fetchByCategoryForDay(d7),
      fetchByCategoryForDay(d14),
      fetchByCategoryForDay(d21),
      fetchByCategoryForDay(d28),
    ]);

    const topProducts = await AppDataSource.query(
      `
      SELECT
        MAX(p.id)::int AS product_id,
        p.sku AS sku,
        MAX(p.name) AS name,
        MAX(p.photo) AS photo,
        MAX(p.url) AS url,
        COALESCE(SUM(i.quantity::int), 0)::int AS qty,
        COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      JOIN order_items i ON i.order_id = o.id
      JOIN products p ON p.id = i.product_id
      WHERE ${whereToday.join(' AND ')}
        AND (CASE WHEN $${dayParamsBase.length + 1}::text[] IS NULL OR array_length($${dayParamsBase.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${dayParamsBase.length + 1}::text[]) END)
        AND (CASE WHEN $${dayParamsBase.length + 2}::text[] IS NULL OR array_length($${dayParamsBase.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${dayParamsBase.length + 2}::text[]) END)
      GROUP BY p.sku
      ORDER BY revenue DESC NULLS LAST
      LIMIT 20
      `,
      [...dayParamsBase, categories.length ? categories : null, skus.length ? skus : null],
    );

    // Normaliza KPIs e calcula "so far" usando currentHour via hourlyRows
    const revenueByDayHour = new Map<string, Map<number, number>>();
    for (const r of hourlyRows || []) {
      const day = String((r as any).day);
      const hour = Number((r as any).hour ?? 0);
      const rev = Number((r as any).revenue ?? 0) || 0;
      if (!revenueByDayHour.has(day)) revenueByDayHour.set(day, new Map());
      revenueByDayHour.get(day)!.set(hour, rev);
    }
    const sumSoFar = (day: string): number => {
      const m = revenueByDayHour.get(day);
      if (!m) return 0;
      let acc = 0;
      for (let h = 0; h <= Math.min(23, Math.max(0, currentHour)); h++) acc += Number(m.get(h) ?? 0) || 0;
      return acc;
    };
    const sumDay = (day: string): number => {
      const m = revenueByDayHour.get(day);
      if (!m) return 0;
      let acc = 0;
      for (let h = 0; h < 24; h++) acc += Number(m.get(h) ?? 0) || 0;
      return acc;
    };

    const kpiByDay = new Map<string, any>();
    for (const r of kpiRows || []) {
      const day = String((r as any).day);
      kpiByDay.set(day, {
        revenue: Number((r as any).revenue ?? 0) || 0,
        orders: Number((r as any).orders ?? 0) || 0,
        customers: Number((r as any).customers ?? 0) || 0,
        itemsSold: Number((r as any).items_sold ?? 0) || 0,
        skusCount: Number((r as any).skus_count ?? 0) || 0,
      });
    }
    if (!useItemsRevenue) {
      for (const r of itemsSoldRows || []) {
        const day = String((r as any).day);
        const cur = kpiByDay.get(day) || { revenue: 0, orders: 0, customers: 0, itemsSold: 0, skusCount: 0 };
        cur.itemsSold = Number((r as any).items_sold ?? 0) || 0;
        kpiByDay.set(day, cur);
      }
      for (const r of skusCountRows || []) {
        const day = String((r as any).day);
        const cur = kpiByDay.get(day) || { revenue: 0, orders: 0, customers: 0, itemsSold: 0, skusCount: 0 };
        cur.skusCount = Number((r as any).skus_count ?? 0) || 0;
        kpiByDay.set(day, cur);
      }
    }
    const makeKpi = (day: string) => {
      const base = kpiByDay.get(day) || { revenue: 0, orders: 0, customers: 0, itemsSold: 0, skusCount: 0 };
      const revenueSoFar = sumSoFar(day);
      const orders = Number(base.orders) || 0;
      return {
        revenueSoFar,
        revenueDay: sumDay(day),
        orders,
        uniqueCustomers: Number(base.customers) || 0,
        itemsSold: Number(base.itemsSold) || 0,
        skusCount: Number(base.skusCount) || 0,
        avgTicket: orders > 0 ? revenueSoFar / orders : 0,
        cartItemsAdded: 0,
        conversionPct: 0,
      };
    };

    const kToday = makeKpi(today);
    const kYesterday = makeKpi(yesterday);
    const kD7 = makeKpi(d7);
    const kD14 = makeKpi(d14);
    const kD21 = makeKpi(d21);
    const kD28 = makeKpi(d28);
    const kD8 = makeKpi(d8);

    // Projeção: mesma lógica do mock (D-7 do dia * crescimento de Ontem vs Ontem (D-7))
    const lastWeekSameDayTotal = kD7.revenueDay;
    const yesterdayTotal = kYesterday.revenueDay;
    const lastWeekYesterdayTotal = kD8.revenueDay;
    const growth = lastWeekYesterdayTotal > 0 ? yesterdayTotal / lastWeekYesterdayTotal : 0;
    const projectedTodayTotal = growth > 0 ? lastWeekSameDayTotal * growth : 0;

    return res.json({
      companyId,
      groupId,
      today,
      currentHour,
      isLive,
      useItemsRevenue,
      kpis: {
        today: kToday,
        yesterday: kYesterday,
        d7: kD7,
        d14: kD14,
        d21: kD21,
        d28: kD28,
      },
      projection: {
        projectedTodayTotal: isLive ? projectedTodayTotal : 0,
      },
      hourly: (hourlyRows || []).map((r: any) => ({
        day: String(r.day),
        hour: Number(r.hour ?? 0) || 0,
        revenue: Number(r.revenue ?? 0) || 0,
      })),
      topProducts: (topProducts || []).map((r: any) => ({
        productId: Number(r.product_id ?? 0) || null,
        sku: String(r.sku ?? ''),
        name: r.name ?? null,
        photo: r.photo ?? null,
        url: r.url ?? null,
        qty: Number(r.qty ?? 0) || 0,
        revenue: Number(r.revenue ?? 0) || 0,
      })),
      byMarketplace: (byMarketplace || []).map((r: any) => ({ id: String(r.id), value: Number(r.value ?? 0) || 0 })),
      byMarketplaceTable: toMarketplaceTable(byMarketplace),
      byMarketplaceTableByPeriod: {
        today: toMarketplaceTable(byMarketplace),
        yesterday: bmYesterday,
        d7: bmD7,
        d14: bmD14,
        d21: bmD21,
        d28: bmD28,
      },
      byCategory: (byCategory || []).map((r: any) => ({ id: String(r.id), value: Number(r.value ?? 0) || 0 })),
      byState: (byState || []).map((r: any) => ({ id: String(r.id), value: Number(r.value ?? 0) || 0 })),
      byCategoryTable: toCategoryTable(byCategory),
      byCategoryTableByPeriod: {
        today: toCategoryTable(byCategory),
        yesterday: catYesterday,
        d7: catD7,
        d14: catD14,
        d21: catD21,
        d28: catD28,
      },
      byStateTable: toStateTable(byState),
      byStateTableByPeriod: {
        today: toStateTable(byState),
        yesterday: stateYesterday,
        d7: stateD7,
        d14: stateD14,
        d21: stateD21,
        d28: stateD28,
      },
      byProductTable: prodToday,
      byProductTableD1: prodYesterday,
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao carregar Ao Vivo' });
  }
});

companiesRouter.get('/me/dashboard/operation/summary', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const param = groupId ?? companyId;

  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = Array.from(
    new Set([...toStringArray((req.query as any)?.sku), ...toStringArray((req.query as any)?.product)].map((s) => String(s).trim()).filter(Boolean)),
  );
  const companyIds = Array.from(new Set([...toStringArray((req.query as any)?.company_id), ...toStringArray((req.query as any)?.store)]))
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  // Para a Operação, as métricas são por status (cancelado/devolvido/andamento),
  // então NÃO aplicamos o filtro "status" do usuário aqui.
  const useItemsFilter = Boolean(categories.length || skus.length);

  const where: string[] = [`${condSql}`, `o.order_date IS NOT NULL`, `o.order_date::date BETWEEN $2::date AND $3::date`];
  const params: any[] = [param, start, end];

  if (groupId && companyIds.length) {
    params.push(companyIds);
    where.push(`o.company_id = ANY($${params.length}::int[])`);
  }
  if (channels.length) {
    params.push(channels);
    where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${params.length}::text[])`);
  }
  if (states.length) {
    params.push(states);
    where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${params.length}::text[])`);
  }
  if (cities.length) {
    params.push(cities);
    where.push(`o.delivery_city = ANY($${params.length}::text[])`);
  }

  const statusExpr = `LOWER(TRIM(COALESCE(o.current_status, '')))`;
  const doneStatuses = ['entregue', 'devolvido', 'cancelado', 'bloqueado'];

  const rows = await AppDataSource.query(
    useItemsFilter
      ? `
      WITH base AS (
        SELECT DISTINCT o.id, ${statusExpr} AS st
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        WHERE ${where.join(' AND ')}
          AND (CASE WHEN $${params.length + 1}::text[] IS NULL OR array_length($${params.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${params.length + 1}::text[]) END)
          AND (CASE WHEN $${params.length + 2}::text[] IS NULL OR array_length($${params.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${params.length + 2}::text[]) END)
      )
      SELECT
        COALESCE(SUM(CASE WHEN st = 'cancelado' THEN 1 ELSE 0 END), 0)::int AS cancelled,
        COALESCE(SUM(CASE WHEN st = 'devolvido' THEN 1 ELSE 0 END), 0)::int AS returned,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 3}::text[]) THEN 0 ELSE 1 END), 0)::int AS in_progress
      FROM base
      `
      : `
      SELECT
        COALESCE(SUM(CASE WHEN ${statusExpr} = 'cancelado' THEN 1 ELSE 0 END), 0)::int AS cancelled,
        COALESCE(SUM(CASE WHEN ${statusExpr} = 'devolvido' THEN 1 ELSE 0 END), 0)::int AS returned,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 1}::text[]) THEN 0 ELSE 1 END), 0)::int AS in_progress
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      WHERE ${where.join(' AND ')}
      `,
    useItemsFilter
      ? [...params, categories.length ? categories : null, skus.length ? skus : null, doneStatuses]
      : [...params, doneStatuses],
  );

  const r0 = rows?.[0] || {};
  return res.json({
    start,
    end,
    cancelled: Number(r0.cancelled ?? 0) || 0,
    returned: Number(r0.returned ?? 0) || 0,
    inProgress: Number(r0.in_progress ?? 0) || 0,
  });
});

companiesRouter.get('/me/dashboard/operation/marketplace-table', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const param = groupId ?? companyId;

  const channels = toStringArray((req.query as any)?.channel);
  const statuses = toStringArray((req.query as any)?.status).map((s) => s.toLowerCase());
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = Array.from(
    new Set([...toStringArray((req.query as any)?.sku), ...toStringArray((req.query as any)?.product)].map((s) => String(s).trim()).filter(Boolean)),
  );
  const companyIds = Array.from(new Set([...toStringArray((req.query as any)?.company_id), ...toStringArray((req.query as any)?.store)]))
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  // Regras
  const toInvoiceStatuses = ['novo', 'aguardando pagamento', 'pendente', 'em analise', 'aprovado']; // "antes de faturar"
  const inTransitStatuses = ['coletando', 'aguardando disponibilidade', 'em transporte', 'entregue marketplace'];
  const cancelBucketStatuses = ['devolvido', 'cancelado', 'frete não atendido', 'desmembrado', 'bloqueado'];

  // Para esse detalhamento, filtros por categoria/SKU precisam olhar itens.
  const useItemsFilter = Boolean(categories.length || skus.length);

  const where: string[] = [
    `${condSql}`,
    `o.order_date IS NOT NULL`,
    `o.order_date::date BETWEEN $2::date AND $3::date`,
  ];
  const params: any[] = [param, start, end];

  if (groupId && companyIds.length) {
    params.push(companyIds);
    where.push(`o.company_id = ANY($${params.length}::int[])`);
  }
  if (channels.length) {
    params.push(channels);
    where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${params.length}::text[])`);
  }
  if (states.length) {
    params.push(states);
    where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${params.length}::text[])`);
  }
  if (cities.length) {
    params.push(cities);
    where.push(`o.delivery_city = ANY($${params.length}::text[])`);
  }
  if (statuses.length) {
    params.push(statuses);
    where.push(`LOWER(TRIM(COALESCE(o.current_status, ''))) = ANY($${params.length}::text[])`);
  }

  const mpExpr = `COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)')`;
  const statusExpr = `LOWER(TRIM(COALESCE(o.current_status, '')))`;

  const rows = await AppDataSource.query(
    useItemsFilter
      ? `
      WITH meta AS (
        SELECT
          (now() AT TIME ZONE 'America/Sao_Paulo')::date AS today,
          ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 1)::date AS yesterday,
          ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 3)::date AS day_3
      ),
      base AS (
        SELECT DISTINCT
          o.id,
          ${mpExpr} AS marketplace,
          o.order_date::date AS order_day,
          ${statusExpr} AS st
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        WHERE ${where.join(' AND ')}
          AND (CASE WHEN $${params.length + 1}::text[] IS NULL OR array_length($${params.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${params.length + 1}::text[]) END)
          AND (CASE WHEN $${params.length + 2}::text[] IS NULL OR array_length($${params.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${params.length + 2}::text[]) END)
      )
      SELECT
        marketplace,
        COUNT(*)::int AS total_orders,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 3}::text[]) THEN 1 ELSE 0 END), 0)::int AS to_invoice,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 3}::text[]) AND order_day = (SELECT today FROM meta) THEN 1 ELSE 0 END), 0)::int AS today_orders,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 3}::text[]) AND order_day = (SELECT yesterday FROM meta) THEN 1 ELSE 0 END), 0)::int AS yesterday_orders,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 3}::text[]) AND order_day <= (SELECT day_3 FROM meta) THEN 1 ELSE 0 END), 0)::int AS above_3_days,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 3}::text[]) THEN 1 ELSE 0 END), 0)::int AS kanban_new,
        COALESCE(SUM(CASE WHEN st = 'faturando' OR st = 'aguardando transporte' THEN 1 ELSE 0 END), 0)::int AS kanban_invoiced,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 4}::text[]) THEN 1 ELSE 0 END), 0)::int AS kanban_in_transit,
        COALESCE(SUM(CASE WHEN st = 'entregue' THEN 1 ELSE 0 END), 0)::int AS kanban_delivered,
        COALESCE(SUM(CASE WHEN st = ANY($${params.length + 5}::text[]) THEN 1 ELSE 0 END), 0)::int AS kanban_cancelled,
        COALESCE(SUM(CASE WHEN st = 'cancelado' THEN 1 ELSE 0 END), 0)::int AS cancelled,
        COALESCE(SUM(CASE WHEN st = 'devolvido' THEN 1 ELSE 0 END), 0)::int AS returned
      FROM base
      GROUP BY 1
      ORDER BY total_orders DESC, marketplace ASC
      `
      : `
      WITH meta AS (
        SELECT
          (now() AT TIME ZONE 'America/Sao_Paulo')::date AS today,
          ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 1)::date AS yesterday,
          ((now() AT TIME ZONE 'America/Sao_Paulo')::date - 3)::date AS day_3
      )
      SELECT
        ${mpExpr} AS marketplace,
        COUNT(DISTINCT o.id)::int AS total_orders,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 1}::text[]) THEN 1 ELSE 0 END), 0)::int AS to_invoice,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 1}::text[]) AND o.order_date::date = (SELECT today FROM meta) THEN 1 ELSE 0 END), 0)::int AS today_orders,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 1}::text[]) AND o.order_date::date = (SELECT yesterday FROM meta) THEN 1 ELSE 0 END), 0)::int AS yesterday_orders,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 1}::text[]) AND o.order_date::date <= (SELECT day_3 FROM meta) THEN 1 ELSE 0 END), 0)::int AS above_3_days,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 1}::text[]) THEN 1 ELSE 0 END), 0)::int AS kanban_new,
        COALESCE(SUM(CASE WHEN ${statusExpr} = 'faturando' OR ${statusExpr} = 'aguardando transporte' THEN 1 ELSE 0 END), 0)::int AS kanban_invoiced,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 2}::text[]) THEN 1 ELSE 0 END), 0)::int AS kanban_in_transit,
        COALESCE(SUM(CASE WHEN ${statusExpr} = 'entregue' THEN 1 ELSE 0 END), 0)::int AS kanban_delivered,
        COALESCE(SUM(CASE WHEN ${statusExpr} = ANY($${params.length + 3}::text[]) THEN 1 ELSE 0 END), 0)::int AS kanban_cancelled,
        COALESCE(SUM(CASE WHEN ${statusExpr} = 'cancelado' THEN 1 ELSE 0 END), 0)::int AS cancelled,
        COALESCE(SUM(CASE WHEN ${statusExpr} = 'devolvido' THEN 1 ELSE 0 END), 0)::int AS returned
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      WHERE ${where.join(' AND ')}
      GROUP BY 1
      ORDER BY total_orders DESC, marketplace ASC
      `,
    useItemsFilter
      ? [...params, categories.length ? categories : null, skus.length ? skus : null, toInvoiceStatuses, inTransitStatuses, cancelBucketStatuses]
      : [...params, toInvoiceStatuses, inTransitStatuses, cancelBucketStatuses],
  );

  const outRows = (rows || []).map((r: any) => ({
    marketplace: String(r.marketplace ?? ''),
    totalOrders: Number(r.total_orders ?? 0) || 0,
    toInvoice: Number(r.to_invoice ?? 0) || 0,
    today: Number(r.today_orders ?? 0) || 0,
    yesterday: Number(r.yesterday_orders ?? 0) || 0,
    above3Days: Number(r.above_3_days ?? 0) || 0,
    kanbanNew: Number(r.kanban_new ?? 0) || 0,
    kanbanInvoiced: Number(r.kanban_invoiced ?? 0) || 0,
    kanbanInTransit: Number(r.kanban_in_transit ?? 0) || 0,
    kanbanDelivered: Number(r.kanban_delivered ?? 0) || 0,
    kanbanCancelled: Number(r.kanban_cancelled ?? 0) || 0,
    cancelled: Number(r.cancelled ?? 0) || 0,
    returned: Number(r.returned ?? 0) || 0,
  }));
  const marketplaces = outRows.map((r: any) => String(r.marketplace));

  return res.json({ start, end, marketplaces, rows: outRows });
});

companiesRouter.get('/me/dashboard/operation/kanban', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }

  const limitRaw = Number((req.query as any)?.limit ?? 100);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 10), 500) : 100;

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const { companyId, groupId } = filter;
  const condSql = groupId ? 'c.group_id = $1' : 'o.company_id = $1';
  const param = groupId ?? companyId;

  const channels = toStringArray((req.query as any)?.channel);
  const statuses = toStringArray((req.query as any)?.status).map((s) => s.toLowerCase());
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = Array.from(
    new Set([...toStringArray((req.query as any)?.sku), ...toStringArray((req.query as any)?.product)].map((s) => String(s).trim()).filter(Boolean)),
  );
  const companyIds = Array.from(new Set([...toStringArray((req.query as any)?.company_id), ...toStringArray((req.query as any)?.store)]))
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  const novos = ['novo', 'aguardando pagamento', 'pendente', 'em analise', 'aprovado'];
  const emTransporte = ['coletando', 'aguardando disponibilidade', 'em transporte', 'entregue marketplace'];
  const cancelados = ['devolvido', 'cancelado', 'frete não atendido', 'desmembrado', 'bloqueado'];

  const useItemsFilter = Boolean(categories.length || skus.length);

  const where: string[] = [`${condSql}`, `o.order_date IS NOT NULL`, `o.order_date::date BETWEEN $2::date AND $3::date`];
  const params: any[] = [param, start, end];

  if (groupId && companyIds.length) {
    params.push(companyIds);
    where.push(`o.company_id = ANY($${params.length}::int[])`);
  }
  if (channels.length) {
    params.push(channels);
    where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${params.length}::text[])`);
  }
  if (states.length) {
    params.push(states);
    where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${params.length}::text[])`);
  }
  if (cities.length) {
    params.push(cities);
    where.push(`o.delivery_city = ANY($${params.length}::text[])`);
  }
  if (statuses.length) {
    params.push(statuses);
    where.push(`LOWER(TRIM(COALESCE(o.current_status, ''))) = ANY($${params.length}::text[])`);
  }

  const mpExpr = `COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)')`;
  const statusExpr = `LOWER(TRIM(COALESCE(o.current_status, '')))`;

  const buildSql = (bucket: 'novos' | 'faturados' | 'em_transporte' | 'entregues' | 'cancelados') => {
    const filt =
      bucket === 'novos'
        ? `st = ANY($${params.length + (useItemsFilter ? 3 : 1)}::text[])`
        : bucket === 'faturados'
          ? `(st = 'faturando' OR st = 'aguardando transporte')`
          : bucket === 'em_transporte'
            ? `st = ANY($${params.length + (useItemsFilter ? 4 : 2)}::text[])`
            : bucket === 'entregues'
              ? `st = 'entregue'`
              : `st = ANY($${params.length + (useItemsFilter ? 5 : 3)}::text[])`;

    const idxNovos = params.length + (useItemsFilter ? 3 : 1);
    const idxTransit = params.length + (useItemsFilter ? 4 : 2);
    const idxCancel = params.length + (useItemsFilter ? 5 : 3);
    const pLimitIdx = idxCancel + 1;
    return useItemsFilter
      ? `
      WITH meta AS (
        SELECT (now() AT TIME ZONE 'America/Sao_Paulo')::date AS today
      ),
      typed AS (
        SELECT
          $${idxNovos}::text[] AS novos,
          $${idxTransit}::text[] AS em_transporte,
          $${idxCancel}::text[] AS cancelados
      ),
      base AS (
        SELECT DISTINCT
          o.id,
          o.order_code,
          ${mpExpr} AS marketplace,
          o.order_date::date AS order_day,
          CASE
            WHEN o.delivery_date IS NOT NULL THEN o.delivery_date::date
            WHEN o.order_date IS NOT NULL AND o.delivery_days IS NOT NULL THEN (o.order_date::date + (o.delivery_days::int * interval '1 day'))::date
            ELSE NULL
          END AS delivery_deadline,
          ${statusExpr} AS st
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        WHERE ${where.join(' AND ')}
          AND (CASE WHEN $${params.length + 1}::text[] IS NULL OR array_length($${params.length + 1}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${params.length + 1}::text[]) END)
          AND (CASE WHEN $${params.length + 2}::text[] IS NULL OR array_length($${params.length + 2}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${params.length + 2}::text[]) END)
      )
      SELECT
        id,
        order_code,
        marketplace,
        to_char(order_day, 'YYYY-MM-DD') AS order_day,
        CASE WHEN delivery_deadline IS NULL THEN NULL ELSE to_char(delivery_deadline, 'YYYY-MM-DD') END AS delivery_deadline,
        GREATEST(((SELECT today FROM meta) - order_day), 0)::int AS days_since_order,
        CASE WHEN delivery_deadline IS NOT NULL AND (SELECT today FROM meta) > delivery_deadline THEN TRUE ELSE FALSE END AS is_overdue,
        st AS status
      FROM base
      CROSS JOIN typed
      WHERE ${filt}
      ORDER BY order_day ASC, order_code ASC
      LIMIT $${pLimitIdx}::int
      `
      : `
      WITH meta AS (
        SELECT (now() AT TIME ZONE 'America/Sao_Paulo')::date AS today
      )
      , typed AS (
        SELECT
          $${idxNovos}::text[] AS novos,
          $${idxTransit}::text[] AS em_transporte,
          $${idxCancel}::text[] AS cancelados
      )
      SELECT
        o.id,
        o.order_code,
        ${mpExpr} AS marketplace,
        to_char(o.order_date::date, 'YYYY-MM-DD') AS order_day,
        CASE
          WHEN o.delivery_date IS NOT NULL THEN to_char(o.delivery_date::date, 'YYYY-MM-DD')
          WHEN o.order_date IS NOT NULL AND o.delivery_days IS NOT NULL THEN to_char((o.order_date::date + (o.delivery_days::int * interval '1 day'))::date, 'YYYY-MM-DD')
          ELSE NULL
        END AS delivery_deadline,
        GREATEST(((SELECT today FROM meta) - o.order_date::date), 0)::int AS days_since_order,
        CASE
          WHEN o.delivery_date IS NOT NULL AND (SELECT today FROM meta) > o.delivery_date::date THEN TRUE
          WHEN o.delivery_date IS NULL AND o.order_date IS NOT NULL AND o.delivery_days IS NOT NULL
               AND (SELECT today FROM meta) > (o.order_date::date + (o.delivery_days::int * interval '1 day'))::date
            THEN TRUE
          ELSE FALSE
        END AS is_overdue,
        ${statusExpr} AS status
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      CROSS JOIN typed
      WHERE ${where.join(' AND ')}
        AND ${filt.split('st').join(statusExpr)}
      ORDER BY o.order_date::date ASC, o.order_code ASC
      LIMIT $${pLimitIdx}::int
      `;
  };

  const rowsByBucketParams = useItemsFilter
    ? [...params, categories.length ? categories : null, skus.length ? skus : null, novos, emTransporte, cancelados, limit]
    : [...params, novos, emTransporte, cancelados, limit];

  const [novosRows, faturadosRows, emTransporteRows, entreguesRows, canceladosRows] = await Promise.all([
    AppDataSource.query(buildSql('novos'), rowsByBucketParams),
    AppDataSource.query(buildSql('faturados'), rowsByBucketParams),
    AppDataSource.query(buildSql('em_transporte'), rowsByBucketParams),
    AppDataSource.query(buildSql('entregues'), rowsByBucketParams),
    AppDataSource.query(buildSql('cancelados'), rowsByBucketParams),
  ]);

  const mapRow = (r: any) => ({
    id: Number(r.id ?? 0) || 0,
    orderCode: Number(r.order_code ?? 0) || 0,
    marketplace: String(r.marketplace ?? ''),
    orderDate: r.order_day ? String(r.order_day) : null,
    deliveryDeadline: r.delivery_deadline ? String(r.delivery_deadline) : null,
    daysSinceOrder: Number(r.days_since_order ?? 0) || 0,
    isOverdue: Boolean(r.is_overdue),
    status: String(r.status ?? ''),
  });

  return res.json({
    start,
    end,
    limit,
    columns: {
      novos: (novosRows || []).map(mapRow),
      faturados: (faturadosRows || []).map(mapRow),
      emTransporte: (emTransporteRows || []).map(mapRow),
      entregues: (entreguesRows || []).map(mapRow),
      cancelados: (canceladosRows || []).map(mapRow),
    },
  });
});

function isIsoYm(value: string): boolean {
  return /^\d{4}-\d{2}$/.test(value);
}

type MonthKpis = {
  revenueSoFar: number;
  revenueMonth: number;
  orders: number;
  uniqueCustomers: number;
  itemsSold: number;
  skusCount: number;
  avgTicket: number;
  cartItemsAdded: number;
  conversionPct: number;
};

companiesRouter.get('/me/dashboard/month/overview', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const { companyId, groupId } = filter;
  const param = groupId ?? companyId;
  const condOrders = groupId ? 'c.group_id = $1' : 'o.company_id = $1';

  const monthRaw = String((req.query as any)?.month ?? '').trim();
  const month = monthRaw ? monthRaw : null;
  if (month && !isIsoYm(month)) return res.status(400).json({ message: 'Parâmetro inválido: month deve estar em YYYY-MM.' });

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = toStringArray((req.query as any)?.sku);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  const useItemsRevenue = Boolean(categories.length || skus.length);

  // Drilldown de categoria (aplica somente nos agregados de categoria)
  type CategoryDrillLevel = 'category' | 'subcategory' | 'final';
  const categoryLevelRaw = String((req.query as any)?.category_level ?? 'category')
    .trim()
    .toLowerCase();
  const categoryLevel: CategoryDrillLevel =
    categoryLevelRaw === 'subcategory' ? 'subcategory' : categoryLevelRaw === 'final' ? 'final' : 'category';
  const drillCategory = String((req.query as any)?.drill_category ?? '').trim();
  const drillSubcategory = String((req.query as any)?.drill_subcategory ?? '').trim();
  const categoryExpr =
    categoryLevel === 'category' ? 'p.category' : categoryLevel === 'subcategory' ? 'p.subcategory' : 'p.final_category';
  const categoryEmptyLabel =
    categoryLevel === 'category' ? '(sem categoria)' : categoryLevel === 'subcategory' ? '(sem subcategoria)' : '(sem categoria final)';
  const categoryIdExpr = `COALESCE(NULLIF(TRIM(${categoryExpr}), ''), '${categoryEmptyLabel}')`;
  const drillCategoryExpr = `COALESCE(NULLIF(TRIM(p.category), ''), '(sem categoria)')`;
  const drillSubcategoryExpr = `COALESCE(NULLIF(TRIM(p.subcategory), ''), '(sem subcategoria)')`;
  const drillCategoryParam = categoryLevel === 'category' ? null : drillCategory || null;
  const drillSubcategoryParam = categoryLevel === 'final' ? drillSubcategory || null : null;

  try {
    const baseDay = month ? `${month}-01` : null;
    const metaRows = await AppDataSource.query(
      `
      WITH x AS (
        SELECT
          (now() AT TIME ZONE 'America/Sao_Paulo')::date AS today,
          COALESCE($1::date, (now() AT TIME ZONE 'America/Sao_Paulo')::date) AS base_day
      ),
      m AS (
        SELECT
          date_trunc('month', base_day)::date AS month_start,
          (date_trunc('month', base_day) + interval '1 month - 1 day')::date AS month_end,
          date_trunc('month', today)::date AS actual_month_start,
          (date_trunc('month', today) + interval '1 month - 1 day')::date AS actual_month_end,
          today
        FROM x
      )
      SELECT
        to_char(month_start, 'YYYY-MM') AS month,
        to_char(month_start, 'YYYY-MM-DD') AS month_start,
        to_char(month_end, 'YYYY-MM-DD') AS month_end,
        EXTRACT(day FROM month_end)::int AS days_in_month,
        (month_start = actual_month_start) AS is_live_month,
        EXTRACT(day FROM today)::int AS today_day,
        to_char((month_start - interval '1 month')::date, 'YYYY-MM-DD') AS m1_start,
        to_char((month_start - interval '1 day')::date, 'YYYY-MM-DD') AS m1_end,
        to_char((month_start - interval '2 months')::date, 'YYYY-MM-DD') AS m2_start,
        to_char((month_start - interval '1 month - 1 day')::date, 'YYYY-MM-DD') AS m2_end,
        to_char((month_start - interval '6 months')::date, 'YYYY-MM-DD') AS m6_start,
        to_char((month_start - interval '5 months - 1 day')::date, 'YYYY-MM-DD') AS m6_end,
        to_char((month_start - interval '12 months')::date, 'YYYY-MM-DD') AS m12_start,
        to_char((month_start - interval '11 months - 1 day')::date, 'YYYY-MM-DD') AS m12_end,
        to_char(actual_month_start, 'YYYY-MM') AS actual_month
      FROM m
      `,
      [baseDay],
    );
    const meta = metaRows?.[0] || {};
    const selectedMonth = String(meta.month || '').trim();
    const actualMonth = String(meta.actual_month || '').trim();
    if (!selectedMonth) return res.status(500).json({ message: 'Falha ao calcular mês.' });
    if (actualMonth && selectedMonth > actualMonth) return res.status(400).json({ message: `month não pode ser maior que o mês atual (${actualMonth}).` });

    const isLiveMonth = Boolean(meta.is_live_month);
    const daysInMonth = Number(meta.days_in_month ?? 30) || 30;
    const todayDay = Number(meta.today_day ?? 1) || 1;
    const currentDay = isLiveMonth ? Math.min(Math.max(todayDay, 1), daysInMonth) : daysInMonth;

    const periods = {
      selected: { start: String(meta.month_start), end: String(meta.month_end) },
      m1: { start: String(meta.m1_start), end: String(meta.m1_end) },
      m2: { start: String(meta.m2_start), end: String(meta.m2_end) },
      m6: { start: String(meta.m6_start), end: String(meta.m6_end) },
      m12: { start: String(meta.m12_start), end: String(meta.m12_end) },
    } as const;

    const buildSoFarEnd = async (start: string, end: string): Promise<string> => {
      // limita pelo final do mês de referência
      const rows = await AppDataSource.query(
        `SELECT to_char(LEAST(($1::date + ($3::int - 1) * interval '1 day')::date, $2::date), 'YYYY-MM-DD') AS sofar_end`,
        [start, end, currentDay],
      );
      return String(rows?.[0]?.sofar_end ?? end);
    };

    const sofarEnds: Record<keyof typeof periods, string> = {
      selected: isLiveMonth ? await buildSoFarEnd(periods.selected.start, periods.selected.end) : periods.selected.end,
      m1: await buildSoFarEnd(periods.m1.start, periods.m1.end),
      m2: await buildSoFarEnd(periods.m2.start, periods.m2.end),
      m6: await buildSoFarEnd(periods.m6.start, periods.m6.end),
      m12: await buildSoFarEnd(periods.m12.start, periods.m12.end),
    };

    const commonWhere: string[] = [`${condOrders}`, `o.order_date IS NOT NULL`];
    const commonParamsBase: any[] = [param];

    if (groupId && companyIds.length) {
      commonParamsBase.push(companyIds);
      commonWhere.push(`o.company_id = ANY($${commonParamsBase.length}::int[])`);
    }
    if (channels.length) {
      commonParamsBase.push(channels);
      commonWhere.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${commonParamsBase.length}::text[])`);
    }
    if (states.length) {
      commonParamsBase.push(states);
      commonWhere.push(`UPPER(TRIM(o.delivery_state)) = ANY($${commonParamsBase.length}::text[])`);
    }
    if (cities.length) {
      commonParamsBase.push(cities);
      commonWhere.push(`o.delivery_city = ANY($${commonParamsBase.length}::text[])`);
    }

    const revenueExpr = `
      (COALESCE(o.total_amount, 0)::numeric
       - COALESCE(o.total_discount, 0)::numeric
       + COALESCE(o.shipping_amount, 0)::numeric)
    `;

    const kpisFor = async (key: keyof typeof periods): Promise<MonthKpis> => {
      const p = periods[key];
      const sofarEnd = sofarEnds[key];

      if (useItemsRevenue) {
        const pStartIdx = commonParamsBase.length + 1;
        const pEndIdx = commonParamsBase.length + 2;
        const pSofarIdx = commonParamsBase.length + 3;
        const pCategoriesIdx = commonParamsBase.length + 4;
        const pSkusIdx = commonParamsBase.length + 5;
        const rows = await AppDataSource.query(
          `
          SELECT
            COALESCE(SUM(CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN (i.unit_price::numeric) * (i.quantity::int) ELSE 0 END), 0)::numeric AS revenue_month,
            COALESCE(SUM(CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pSofarIdx}::date THEN (i.unit_price::numeric) * (i.quantity::int) ELSE 0 END), 0)::numeric AS revenue_sofar,
            COUNT(DISTINCT CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN o.id END)::int AS orders,
            COUNT(DISTINCT CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN o.customer_id END)::int AS customers,
            COALESCE(SUM(CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN (i.quantity::int) ELSE 0 END), 0)::int AS items_sold,
            COUNT(DISTINCT CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN p.sku END)::int AS skus_count
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${commonWhere.join(' AND ')}
            AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
            AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
          `,
          [...commonParamsBase, p.start, p.end, sofarEnd, categories.length ? categories : null, skus.length ? skus : null],
        );
        const r = rows?.[0] || {};
        const revenueMonth = Number(r.revenue_month ?? 0) || 0;
        const orders = Number(r.orders ?? 0) || 0;
        return {
          revenueSoFar: Number(r.revenue_sofar ?? 0) || 0,
          revenueMonth,
          orders,
          uniqueCustomers: Number(r.customers ?? 0) || 0,
          itemsSold: Number(r.items_sold ?? 0) || 0,
          skusCount: Number(r.skus_count ?? 0) || 0,
          avgTicket: orders > 0 ? revenueMonth / orders : 0,
          cartItemsAdded: 0,
          conversionPct: 0,
        };
      }

      const pStartIdx = commonParamsBase.length + 1;
      const pEndIdx = commonParamsBase.length + 2;
      const pSofarIdx = commonParamsBase.length + 3;
      const rows = await AppDataSource.query(
        `
        SELECT
          COALESCE(SUM(CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN ${revenueExpr} ELSE 0 END), 0)::numeric AS revenue_month,
          COALESCE(SUM(CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pSofarIdx}::date THEN ${revenueExpr} ELSE 0 END), 0)::numeric AS revenue_sofar,
          COUNT(DISTINCT CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN o.id END)::int AS orders,
          COUNT(DISTINCT CASE WHEN o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date THEN o.customer_id END)::int AS customers
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        WHERE ${commonWhere.join(' AND ')}
        `,
        [...commonParamsBase, p.start, p.end, sofarEnd],
      );
      const r = rows?.[0] || {};
      const revenueMonth = Number(r.revenue_month ?? 0) || 0;
      const orders = Number(r.orders ?? 0) || 0;

      const iStartIdx = commonParamsBase.length + 1;
      const iEndIdx = commonParamsBase.length + 2;
      const itemsRows = await AppDataSource.query(
        `
        SELECT
          COALESCE(SUM(i.quantity::int), 0)::int AS items_sold,
          COUNT(DISTINCT p.sku)::int AS skus_count
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        WHERE ${commonWhere.join(' AND ')}
          AND o.order_date::date BETWEEN $${iStartIdx}::date AND $${iEndIdx}::date
        `,
        [...commonParamsBase, p.start, p.end],
      );
      const ir = itemsRows?.[0] || {};

      return {
        revenueSoFar: Number(r.revenue_sofar ?? 0) || 0,
        revenueMonth,
        orders,
        uniqueCustomers: Number(r.customers ?? 0) || 0,
        itemsSold: Number(ir.items_sold ?? 0) || 0,
        skusCount: Number(ir.skus_count ?? 0) || 0,
        avgTicket: orders > 0 ? revenueMonth / orders : 0,
        cartItemsAdded: 0,
        conversionPct: 0,
      };
    };

    const dailyFor = async (key: keyof typeof periods) => {
      const p = periods[key];
      if (useItemsRevenue) {
        const pStartIdx = commonParamsBase.length + 1;
        const pEndIdx = commonParamsBase.length + 2;
        const pCategoriesIdx = commonParamsBase.length + 3;
        const pSkusIdx = commonParamsBase.length + 4;
        const rows = await AppDataSource.query(
          `
          WITH days AS (
            SELECT generate_series($${pStartIdx}::date, $${pEndIdx}::date, '1 day'::interval)::date AS day
          ),
          agg AS (
            SELECT o.order_date::date AS day, COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            JOIN order_items i ON i.order_id = o.id
            JOIN products p ON p.id = i.product_id
            WHERE ${commonWhere.join(' AND ')}
              AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
              AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
              AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
            GROUP BY 1
          )
          SELECT EXTRACT(day FROM d.day)::int AS day, COALESCE(a.revenue, 0)::numeric AS revenue
          FROM days d
          LEFT JOIN agg a ON a.day = d.day
          ORDER BY d.day ASC
          `,
          [...commonParamsBase, p.start, p.end, categories.length ? categories : null, skus.length ? skus : null],
        );
        return (rows || []).map((r: any) => ({ period: key, day: Number(r.day ?? 0) || 0, revenue: Number(r.revenue ?? 0) || 0 }));
      }

      const pStartIdx = commonParamsBase.length + 1;
      const pEndIdx = commonParamsBase.length + 2;
      const rows = await AppDataSource.query(
        `
        WITH days AS (
          SELECT generate_series($${pStartIdx}::date, $${pEndIdx}::date, '1 day'::interval)::date AS day
        ),
        agg AS (
          SELECT o.order_date::date AS day, COALESCE(SUM(${revenueExpr}), 0)::numeric AS revenue
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          WHERE ${commonWhere.join(' AND ')}
            AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
          GROUP BY 1
        )
        SELECT EXTRACT(day FROM d.day)::int AS day, COALESCE(a.revenue, 0)::numeric AS revenue
        FROM days d
        LEFT JOIN agg a ON a.day = d.day
        ORDER BY d.day ASC
        `,
        [...commonParamsBase, p.start, p.end],
      );
      return (rows || []).map((r: any) => ({ period: key, day: Number(r.day ?? 0) || 0, revenue: Number(r.revenue ?? 0) || 0 }));
    };

    const marketplaceTableFor = async (key: keyof typeof periods) => {
      const p = periods[key];
      if (useItemsRevenue) {
        const pStartIdx = commonParamsBase.length + 1;
        const pEndIdx = commonParamsBase.length + 2;
        const pCategoriesIdx = commonParamsBase.length + 3;
        const pSkusIdx = commonParamsBase.length + 4;
        const rows = await AppDataSource.query(
          `
          SELECT
            COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)') AS id,
            COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue,
            COUNT(DISTINCT o.id)::int AS orders_count,
            CASE WHEN COUNT(DISTINCT o.id) > 0 THEN (COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric / COUNT(DISTINCT o.id)) ELSE 0 END AS avg_ticket
          FROM orders o
          JOIN companies c ON c.id = o.company_id
          JOIN order_items i ON i.order_id = o.id
          JOIN products p ON p.id = i.product_id
          WHERE ${commonWhere.join(' AND ')}
            AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
            AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
            AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
          GROUP BY 1
          ORDER BY revenue DESC NULLS LAST
          LIMIT 50
          `,
          [...commonParamsBase, p.start, p.end, categories.length ? categories : null, skus.length ? skus : null],
        );
        return (rows || []).map((r: any) => ({ id: String(r.id), revenue: Number(r.revenue ?? 0) || 0, ordersCount: Number(r.orders_count ?? 0) || 0, avgTicket: Number(r.avg_ticket ?? 0) || 0 }));
      }
      const pStartIdx = commonParamsBase.length + 1;
      const pEndIdx = commonParamsBase.length + 2;
      const rows = await AppDataSource.query(
        `
        SELECT
          COALESCE(NULLIF(TRIM(COALESCE(o.marketplace_name, o.channel)), ''), '(sem marketplace)') AS id,
          COALESCE(SUM(${revenueExpr}), 0)::numeric AS revenue,
          COUNT(DISTINCT o.id)::int AS orders_count,
          CASE WHEN COUNT(DISTINCT o.id) > 0 THEN (COALESCE(SUM(${revenueExpr}), 0)::numeric / COUNT(DISTINCT o.id)) ELSE 0 END AS avg_ticket
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        WHERE ${commonWhere.join(' AND ')}
          AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
        GROUP BY 1
        ORDER BY revenue DESC NULLS LAST
        LIMIT 50
        `,
        [...commonParamsBase, p.start, p.end],
      );
      return (rows || []).map((r: any) => ({ id: String(r.id), revenue: Number(r.revenue ?? 0) || 0, ordersCount: Number(r.orders_count ?? 0) || 0, avgTicket: Number(r.avg_ticket ?? 0) || 0 }));
    };

    const stateTableFor = async (key: keyof typeof periods) => {
      const p = periods[key];
      const pStartIdx = commonParamsBase.length + 1;
      const pEndIdx = commonParamsBase.length + 2;
      const pCategoriesIdx = commonParamsBase.length + 3;
      const pSkusIdx = commonParamsBase.length + 4;
      const rows = await AppDataSource.query(
        useItemsRevenue
          ? `
            SELECT
              COALESCE(NULLIF(TRIM(UPPER(o.delivery_state)), ''), '(sem UF)') AS id,
              COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue,
              COUNT(DISTINCT o.id)::int AS orders_count,
              CASE WHEN COUNT(DISTINCT o.id) > 0 THEN (COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric / COUNT(DISTINCT o.id)) ELSE 0 END AS avg_ticket
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            JOIN order_items i ON i.order_id = o.id
            JOIN products p ON p.id = i.product_id
            WHERE ${commonWhere.join(' AND ')}
              AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
              AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
              AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
            GROUP BY 1
            ORDER BY revenue DESC NULLS LAST
            LIMIT 50
          `
          : `
            SELECT
              COALESCE(NULLIF(TRIM(UPPER(o.delivery_state)), ''), '(sem UF)') AS id,
              COALESCE(SUM(${revenueExpr}), 0)::numeric AS revenue,
              COUNT(DISTINCT o.id)::int AS orders_count,
              CASE WHEN COUNT(DISTINCT o.id) > 0 THEN (COALESCE(SUM(${revenueExpr}), 0)::numeric / COUNT(DISTINCT o.id)) ELSE 0 END AS avg_ticket
            FROM orders o
            JOIN companies c ON c.id = o.company_id
            WHERE ${commonWhere.join(' AND ')}
              AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
            GROUP BY 1
            ORDER BY revenue DESC NULLS LAST
            LIMIT 50
          `,
        useItemsRevenue ? [...commonParamsBase, p.start, p.end, categories.length ? categories : null, skus.length ? skus : null] : [...commonParamsBase, p.start, p.end],
      );
      return (rows || []).map((r: any) => ({ id: String(r.id), revenue: Number(r.revenue ?? 0) || 0, ordersCount: Number(r.orders_count ?? 0) || 0, avgTicket: Number(r.avg_ticket ?? 0) || 0 }));
    };

    const categoryTableFor = async (key: keyof typeof periods) => {
      const p = periods[key];
      const pStartIdx = commonParamsBase.length + 1;
      const pEndIdx = commonParamsBase.length + 2;
      const pCategoriesIdx = commonParamsBase.length + 3;
      const pSkusIdx = commonParamsBase.length + 4;
      const pDrillCategoryIdx = commonParamsBase.length + 5;
      const pDrillSubcategoryIdx = commonParamsBase.length + 6;
      const rows = await AppDataSource.query(
        `
        SELECT
          ${categoryIdExpr} AS id,
          COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue,
          COUNT(DISTINCT o.id)::int AS orders_count,
          CASE WHEN COUNT(DISTINCT o.id) > 0 THEN (COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric / COUNT(DISTINCT o.id)) ELSE 0 END AS avg_ticket
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        WHERE ${commonWhere.join(' AND ')}
          AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
          AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
          AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
          AND (CASE WHEN $${pDrillCategoryIdx}::text IS NULL OR $${pDrillCategoryIdx}::text = '' THEN TRUE ELSE ${drillCategoryExpr} = $${pDrillCategoryIdx}::text END)
          AND (CASE WHEN $${pDrillSubcategoryIdx}::text IS NULL OR $${pDrillSubcategoryIdx}::text = '' THEN TRUE ELSE ${drillSubcategoryExpr} = $${pDrillSubcategoryIdx}::text END)
        GROUP BY 1
        ORDER BY revenue DESC NULLS LAST
        LIMIT 50
        `,
        [
          ...commonParamsBase,
          p.start,
          p.end,
          categories.length ? categories : null,
          skus.length ? skus : null,
          drillCategoryParam,
          drillSubcategoryParam,
        ],
      );
      return (rows || []).map((r: any) => ({ id: String(r.id), revenue: Number(r.revenue ?? 0) || 0, ordersCount: Number(r.orders_count ?? 0) || 0, avgTicket: Number(r.avg_ticket ?? 0) || 0 }));
    };

    const [kSelected, kM1, kM2, kM6, kM12] = await Promise.all([kpisFor('selected'), kpisFor('m1'), kpisFor('m2'), kpisFor('m6'), kpisFor('m12')]);
    const [dSelected, dM1, dM2, dM6, dM12] = await Promise.all([
      dailyFor('selected'),
      dailyFor('m1'),
      dailyFor('m2'),
      dailyFor('m6'),
      dailyFor('m12'),
    ]);

    const projectedMonthTotal = isLiveMonth && currentDay > 0 ? (kSelected.revenueSoFar * daysInMonth) / currentDay : kSelected.revenueMonth;

    const [mpSel, mpM1, mpM2, mpM6, mpM12] = await Promise.all([
      marketplaceTableFor('selected'),
      marketplaceTableFor('m1'),
      marketplaceTableFor('m2'),
      marketplaceTableFor('m6'),
      marketplaceTableFor('m12'),
    ]);
    const [stSel, stM1, stM2, stM6, stM12] = await Promise.all([
      stateTableFor('selected'),
      stateTableFor('m1'),
      stateTableFor('m2'),
      stateTableFor('m6'),
      stateTableFor('m12'),
    ]);
    const [catSel, catM1, catM2, catM6, catM12] = await Promise.all([
      categoryTableFor('selected'),
      categoryTableFor('m1'),
      categoryTableFor('m2'),
      categoryTableFor('m6'),
      categoryTableFor('m12'),
    ]);

    const pStartIdx = commonParamsBase.length + 1;
    const pEndIdx = commonParamsBase.length + 2;
    const pCategoriesIdx = commonParamsBase.length + 3;
    const pSkusIdx = commonParamsBase.length + 4;
    const topProducts = await AppDataSource.query(
      `
      SELECT
        MAX(p.id)::int AS product_id,
        p.sku AS sku,
        MAX(p.name) AS name,
        MAX(p.photo) AS photo,
        MAX(p.url) AS url,
        COALESCE(SUM(i.quantity::int), 0)::int AS qty,
        COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
      FROM orders o
      JOIN companies c ON c.id = o.company_id
      JOIN order_items i ON i.order_id = o.id
      JOIN products p ON p.id = i.product_id
      WHERE ${commonWhere.join(' AND ')}
        AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
        AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
        AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
      GROUP BY p.sku
      ORDER BY revenue DESC NULLS LAST
      LIMIT 20
      `,
      [...commonParamsBase, periods.selected.start, periods.selected.end, categories.length ? categories : null, skus.length ? skus : null],
    );

    const productTableFor = async (key: keyof typeof periods) => {
      const p = periods[key];
      const pStartIdx = commonParamsBase.length + 1;
      const pEndIdx = commonParamsBase.length + 2;
      const pCategoriesIdx = commonParamsBase.length + 3;
      const pSkusIdx = commonParamsBase.length + 4;
      const rows = await AppDataSource.query(
        `
        SELECT
          MAX(p.id)::int AS product_id,
          p.sku AS sku,
          MAX(p.name) AS name,
          COUNT(DISTINCT o.id)::int AS orders_count,
          COALESCE(SUM((i.unit_price::numeric) * (i.quantity::int)), 0)::numeric AS revenue
        FROM orders o
        JOIN companies c ON c.id = o.company_id
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        WHERE ${commonWhere.join(' AND ')}
          AND o.order_date::date BETWEEN $${pStartIdx}::date AND $${pEndIdx}::date
          AND (CASE WHEN $${pCategoriesIdx}::text[] IS NULL OR array_length($${pCategoriesIdx}::text[], 1) IS NULL THEN TRUE ELSE COALESCE(p.final_category, p.category) = ANY($${pCategoriesIdx}::text[]) END)
          AND (CASE WHEN $${pSkusIdx}::text[] IS NULL OR array_length($${pSkusIdx}::text[], 1) IS NULL THEN TRUE ELSE p.sku = ANY($${pSkusIdx}::text[]) END)
        GROUP BY p.sku
        ORDER BY revenue DESC NULLS LAST, sku ASC
        LIMIT 500
        `,
        [...commonParamsBase, p.start, sofarEnds[key], categories.length ? categories : null, skus.length ? skus : null],
      );

      return (rows || []).map((r: any) => {
        const revenue = Number(r.revenue ?? 0) || 0;
        const ordersCount = Number(r.orders_count ?? 0) || 0;
        return {
          productId: Number(r.product_id ?? 0) || null,
          sku: String(r.sku ?? ''),
          name: r.name ?? null,
          revenue,
          ordersCount,
          avgTicket: ordersCount > 0 ? revenue / ordersCount : 0,
        };
      });
    };

    // Tabela final de SKUs (fixa: mês selecionado vs M-1, ambos "até agora" quando mês ao vivo)
    const [prodSelected, prodM1] = await Promise.all([productTableFor('selected'), productTableFor('m1')]);

    return res.json({
      month: selectedMonth,
      isLiveMonth,
      currentDay,
      daysInMonth,
      projection: { projectedMonthTotal },
      kpis: { selected: kSelected, m1: kM1, m2: kM2, m6: kM6, m12: kM12 },
      daily: [...dSelected, ...dM1, ...dM2, ...dM6, ...dM12],
      topProducts: (topProducts || []).map((r: any) => ({
        productId: Number(r.product_id ?? 0) || null,
        sku: String(r.sku ?? ''),
        name: r.name ?? null,
        photo: r.photo ?? null,
        url: r.url ?? null,
        qty: Number(r.qty ?? 0) || 0,
        revenue: Number(r.revenue ?? 0) || 0,
      })),
      byMarketplace: (mpSel || []).map((r: any) => ({ id: String(r.id), value: Number(r.revenue ?? 0) || 0 })),
      byMarketplaceTableByPeriod: { selected: mpSel, m1: mpM1, m2: mpM2, m6: mpM6, m12: mpM12 },
      byState: (stSel || []).map((r: any) => ({ id: String(r.id), value: Number(r.revenue ?? 0) || 0 })),
      byStateTableByPeriod: { selected: stSel, m1: stM1, m2: stM2, m6: stM6, m12: stM12 },
      byCategory: (catSel || []).map((r: any) => ({ id: String(r.id), value: Number(r.revenue ?? 0) || 0 })),
      byCategoryTableByPeriod: { selected: catSel, m1: catM1, m2: catM2, m6: catM6, m12: catM12 },
      byProductTable: prodSelected,
      byProductTableM1: prodM1,
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao carregar mês (overview)' });
  }
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

companiesRouter.get('/me/dashboard/items/overview', async (req: Request, res: Response) => {
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
  const condOrders = groupId ? 'c.group_id = $1' : 'o.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const statuses = toStringArray((req.query as any)?.status);
  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = toStringArray((req.query as any)?.sku);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  try {
    const params: any[] = [param, start, end];
    const where: string[] = [
      `${condOrders}`,
      `o.order_date IS NOT NULL`,
      `o.order_date::date BETWEEN $2::date AND $3::date`,
    ];

    if (companyIds.length) {
      params.push(companyIds);
      where.push(`o.company_id = ANY($${params.length}::int[])`);
    }
    if (statuses.length) {
      params.push(statuses);
      where.push(`o.current_status = ANY($${params.length}::text[])`);
    }
    if (channels.length) {
      params.push(channels);
      where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${params.length}::text[])`);
    }
    if (states.length) {
      params.push(states);
      where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${params.length}::text[])`);
    }
    if (cities.length) {
      params.push(cities);
      where.push(`o.delivery_city = ANY($${params.length}::text[])`);
    }
    if (categories.length) {
      params.push(categories);
      where.push(`COALESCE(p.final_category, p.category) = ANY($${params.length}::text[])`);
    }
    if (skus.length) {
      params.push(skus);
      where.push(`p.sku = ANY($${params.length}::text[])`);
    }

    const sqlBase = `
      WITH base AS (
        SELECT
          o.order_date::date AS day,
          o.channel AS channel,
          COALESCE(o.marketplace_name, o.channel) AS marketplace,
          UPPER(TRIM(o.delivery_state)) AS delivery_state,
          o.delivery_city AS delivery_city,
          o.current_status AS current_status,
          (i.unit_price::numeric) AS unit_price,
          (i.quantity::int) AS quantity,
          ((i.unit_price::numeric) * (i.quantity::int)) AS revenue,
          COALESCE(NULLIF(TRIM(p.category), ''), '(sem categoria)') AS category,
          COALESCE(NULLIF(TRIM(p.subcategory), ''), '(sem sub-categoria)') AS subcategory,
          COALESCE(NULLIF(TRIM(p.final_category), ''), NULLIF(TRIM(p.category), ''), '(sem categoria)') AS final_category,
          COALESCE(NULLIF(TRIM(p.brand), ''), '(sem marca)') AS brand,
          p.sku AS sku,
          p.name AS name
        FROM orders o
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        JOIN companies c ON c.id = o.company_id
        WHERE ${where.join(' AND ')}
      ),
      top_final AS (
        SELECT final_category, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT 6
      ),
      top_category AS (
        SELECT category, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT 6
      ),
      days AS (
        SELECT generate_series($2::date, $3::date, '1 day'::interval)::date AS day
      ),
      daily AS (
        SELECT
          to_char(d.day, 'DD/MM') AS date,
          tf.final_category AS final_category,
          COALESCE(SUM(b.revenue), 0)::numeric AS revenue
        FROM days d
        JOIN top_final tf ON TRUE
        LEFT JOIN base b ON b.day = d.day AND b.final_category = tf.final_category
        GROUP BY d.day, tf.final_category
        ORDER BY d.day ASC
      ),
      daily_category AS (
        SELECT
          to_char(d.day, 'DD/MM') AS date,
          tc.category AS category,
          COALESCE(SUM(b.revenue), 0)::numeric AS revenue
        FROM days d
        JOIN top_category tc ON TRUE
        LEFT JOIN base b ON b.day = d.day AND b.category = tc.category
        GROUP BY d.day, tc.category
        ORDER BY d.day ASC
      ),
      category_totals AS (
        SELECT category, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
      ),
      final_category_totals AS (
        SELECT category, final_category, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1, 2
        ORDER BY revenue DESC
      ),
      top_products AS (
        SELECT
          sku,
          MAX(name) AS name,
          SUM(quantity)::int AS qty,
          SUM(revenue)::numeric AS revenue
        FROM base
        WHERE sku IS NOT NULL AND TRIM(sku) <> ''
        GROUP BY 1
        ORDER BY revenue DESC
        LIMIT 20
      ),
      by_channel AS (
        -- "Marketplace" vem de orders.marketplace_name (fallback para orders.channel)
        SELECT COALESCE(NULLIF(TRIM(marketplace), ''), '(sem marketplace)') AS channel, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
      ),
      by_brand AS (
        SELECT brand, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
      ),
      by_state AS (
        SELECT COALESCE(NULLIF(TRIM(delivery_state), ''), '(sem UF)') AS state, SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1
        ORDER BY revenue DESC
      ),
      treemap_rows AS (
        SELECT
          category,
          subcategory,
          final_category,
          COALESCE(NULLIF(TRIM(sku), ''), '(sem sku)') AS sku,
          MAX(name) AS name,
          SUM(revenue)::numeric AS revenue
        FROM base
        GROUP BY 1,2,3,4
        ORDER BY revenue DESC
        LIMIT 2500
      )
      SELECT
        (SELECT jsonb_agg(jsonb_build_object('final_category', tf.final_category, 'revenue', tf.revenue) ORDER BY tf.revenue DESC) FROM top_final tf) AS top_final_categories,
        (SELECT jsonb_agg(jsonb_build_object('date', d.date, 'final_category', d.final_category, 'revenue', d.revenue) ORDER BY d.date ASC) FROM daily d) AS daily_by_final_category,
        (SELECT jsonb_agg(jsonb_build_object('category', tc.category, 'revenue', tc.revenue) ORDER BY tc.revenue DESC) FROM top_category tc) AS top_categories,
        (SELECT jsonb_agg(jsonb_build_object('date', d.date, 'category', d.category, 'revenue', d.revenue) ORDER BY d.date ASC) FROM daily_category d) AS daily_by_category,
        (SELECT jsonb_agg(jsonb_build_object('category', ct.category, 'revenue', ct.revenue) ORDER BY ct.revenue DESC) FROM category_totals ct) AS category_totals,
        (SELECT jsonb_agg(jsonb_build_object('category', fct.category, 'final_category', fct.final_category, 'revenue', fct.revenue) ORDER BY fct.revenue DESC) FROM final_category_totals fct) AS final_category_totals,
        (SELECT jsonb_agg(jsonb_build_object('sku', tp.sku, 'name', tp.name, 'qty', tp.qty, 'revenue', tp.revenue) ORDER BY tp.revenue DESC) FROM top_products tp) AS top_products,
        (SELECT jsonb_agg(jsonb_build_object('channel', bc.channel, 'revenue', bc.revenue) ORDER BY bc.revenue DESC) FROM by_channel bc) AS by_channel,
        (SELECT jsonb_agg(jsonb_build_object('brand', bb.brand, 'revenue', bb.revenue) ORDER BY bb.revenue DESC) FROM by_brand bb) AS by_brand,
        (SELECT jsonb_agg(jsonb_build_object('state', bs.state, 'revenue', bs.revenue) ORDER BY bs.revenue DESC) FROM by_state bs) AS by_state,
        (SELECT jsonb_agg(jsonb_build_object('category', tr.category, 'subcategory', tr.subcategory, 'final_category', tr.final_category, 'sku', tr.sku, 'name', tr.name, 'revenue', tr.revenue) ORDER BY tr.revenue DESC) FROM treemap_rows tr) AS treemap_rows
    `;

    const [row] = await AppDataSource.query(sqlBase, params);
    return res.json({
      companyId,
      groupId,
      start,
      end,
      topFinalCategories: row?.top_final_categories ?? [],
      dailyByFinalCategory: row?.daily_by_final_category ?? [],
      topCategories: row?.top_categories ?? [],
      dailyByCategory: row?.daily_by_category ?? [],
      categoryTotals: row?.category_totals ?? [],
      finalCategoryTotals: row?.final_category_totals ?? [],
      topProducts: row?.top_products ?? [],
      byChannel: row?.by_channel ?? [],
      byBrand: row?.by_brand ?? [],
      byState: row?.by_state ?? [],
      treemapRows: row?.treemap_rows ?? [],
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao carregar itens (overview)' });
  }
});

// Tabela de SKUs: menor/maior preço e delta de preço médio (primeiro dia vs último dia no período)
companiesRouter.get('/me/dashboard/items/sku-price-table', async (req: Request, res: Response) => {
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
  const condOrders = groupId ? 'c.group_id = $1' : 'o.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const statuses = toStringArray((req.query as any)?.status);
  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const skus = toStringArray((req.query as any)?.sku);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  try {
    const params: any[] = [param, start, end];
    const where: string[] = [
      `${condOrders}`,
      `o.order_date IS NOT NULL`,
      `o.order_date::date BETWEEN $2::date AND $3::date`,
    ];

    if (companyIds.length) {
      params.push(companyIds);
      where.push(`o.company_id = ANY($${params.length}::int[])`);
    }
    if (statuses.length) {
      params.push(statuses);
      where.push(`o.current_status = ANY($${params.length}::text[])`);
    }
    if (channels.length) {
      params.push(channels);
      where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${params.length}::text[])`);
    }
    if (states.length) {
      params.push(states);
      where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${params.length}::text[])`);
    }
    if (cities.length) {
      params.push(cities);
      where.push(`o.delivery_city = ANY($${params.length}::text[])`);
    }
    if (categories.length) {
      params.push(categories);
      where.push(`COALESCE(p.final_category, p.category) = ANY($${params.length}::text[])`);
    }
    if (skus.length) {
      params.push(skus);
      where.push(`p.sku = ANY($${params.length}::text[])`);
    }

    const sql = `
      WITH base AS (
        SELECT
          o.order_date::date AS day,
          p.sku AS sku,
          p.name AS name,
          (i.unit_price::numeric) AS unit_price,
          COALESCE(i.quantity::int, 0) AS quantity,
          ((i.unit_price::numeric) * COALESCE(i.quantity::int, 0)) AS revenue
        FROM orders o
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        JOIN companies c ON c.id = o.company_id
        WHERE ${where.join(' AND ')}
          AND p.sku IS NOT NULL AND TRIM(p.sku) <> ''
          AND i.unit_price IS NOT NULL
      ),
      daily AS (
        SELECT
          sku,
          MAX(name) AS name,
          day,
          SUM(quantity)::int AS qty,
          (SUM(revenue) / NULLIF(SUM(quantity), 0))::numeric AS avg_price
        FROM base
        GROUP BY 1, 3
      ),
      first_last AS (
        SELECT sku, MIN(day) AS first_day, MAX(day) AS last_day
        FROM daily
        GROUP BY 1
      ),
      first_price AS (
        SELECT d.sku, d.avg_price AS first_avg_price
        FROM daily d
        JOIN first_last fl ON fl.sku = d.sku AND fl.first_day = d.day
      ),
      last_price AS (
        SELECT d.sku, d.avg_price AS last_avg_price
        FROM daily d
        JOIN first_last fl ON fl.sku = d.sku AND fl.last_day = d.day
      ),
      agg AS (
        SELECT
          sku,
          MAX(name) AS name,
          SUM(quantity)::int AS qty,
          SUM(revenue)::numeric AS revenue,
          MIN(unit_price)::numeric AS min_price,
          MAX(unit_price)::numeric AS max_price
        FROM base
        GROUP BY 1
      )
      SELECT
        a.sku,
        a.name,
        a.qty,
        a.revenue,
        a.min_price,
        a.max_price,
        fp.first_avg_price,
        lp.last_avg_price,
        CASE
          WHEN a.max_price IS NULL OR a.max_price <= 0 THEN NULL
          WHEN a.min_price IS NULL THEN NULL
          ELSE ((a.max_price - a.min_price) / a.max_price)
        END AS delta_pct
      FROM agg a
      LEFT JOIN first_price fp ON fp.sku = a.sku
      LEFT JOIN last_price lp ON lp.sku = a.sku
      ORDER BY a.revenue DESC NULLS LAST, a.sku ASC
      LIMIT 500
    `;

    const rows = await AppDataSource.query(sql, params);
    return res.json({
      start,
      end,
      rows: (rows || []).map((r: any) => ({
        sku: String(r.sku ?? ''),
        name: r.name ?? null,
        qty: Number(r.qty ?? 0) || 0,
        revenue: String(r.revenue ?? '0'),
        minPrice: String(r.min_price ?? '0'),
        maxPrice: String(r.max_price ?? '0'),
        firstAvgPrice: r.first_avg_price === null ? null : String(r.first_avg_price),
        lastAvgPrice: r.last_avg_price === null ? null : String(r.last_avg_price),
        deltaPct: r.delta_pct === null ? null : Number(r.delta_pct),
      })),
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao carregar tabela de SKUs' });
  }
});

// Série diária de um SKU: quantidade (barras) + preço médio (linha)
companiesRouter.get('/me/dashboard/items/sku-daily', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });

  const filter = await getAuthCompanyGroupFilter(req);
  if (!filter) return res.status(400).json({ message: 'Company not configured for this user' });

  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  const sku = String((req.query as any)?.sku ?? '').trim();
  if (!isIsoYmd(start) || !isIsoYmd(end)) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }
  if (!sku) return res.status(400).json({ message: 'Parâmetro obrigatório: sku' });

  const { companyId, groupId } = filter;
  const param = groupId ?? companyId;
  const condOrders = groupId ? 'c.group_id = $1' : 'o.company_id = $1';

  const toStringArray = (v: any): string[] => {
    if (!v) return [];
    const raw = Array.isArray(v) ? v : [v];
    return raw.map((x) => String(x).trim()).filter(Boolean);
  };

  const statuses = toStringArray((req.query as any)?.status);
  const channels = toStringArray((req.query as any)?.channel);
  const categories = toStringArray((req.query as any)?.category);
  const states = toStringArray((req.query as any)?.state).map((s) => s.toUpperCase());
  const cities = toStringArray((req.query as any)?.city);
  const companyIds = toStringArray((req.query as any)?.company_id)
    .map((s) => Number(s))
    .filter((n) => Number.isInteger(n) && n > 0);

  try {
    const params: any[] = [param, start, end, sku];
    const where: string[] = [
      `${condOrders}`,
      `o.order_date IS NOT NULL`,
      `o.order_date::date BETWEEN $2::date AND $3::date`,
      `p.sku = $4::text`,
    ];

    if (companyIds.length) {
      params.push(companyIds);
      where.push(`o.company_id = ANY($${params.length}::int[])`);
    }
    if (statuses.length) {
      params.push(statuses);
      where.push(`o.current_status = ANY($${params.length}::text[])`);
    }
    if (channels.length) {
      params.push(channels);
      where.push(`COALESCE(o.marketplace_name, o.channel) = ANY($${params.length}::text[])`);
    }
    if (states.length) {
      params.push(states);
      where.push(`UPPER(TRIM(o.delivery_state)) = ANY($${params.length}::text[])`);
    }
    if (cities.length) {
      params.push(cities);
      where.push(`o.delivery_city = ANY($${params.length}::text[])`);
    }
    if (categories.length) {
      params.push(categories);
      where.push(`COALESCE(p.final_category, p.category) = ANY($${params.length}::text[])`);
    }

    const sql = `
      WITH days AS (
        SELECT generate_series($2::date, $3::date, '1 day'::interval)::date AS day
      ),
      base AS (
        SELECT
          o.order_date::date AS day,
          (i.unit_price::numeric) AS unit_price,
          COALESCE(i.quantity::int, 0) AS quantity,
          ((i.unit_price::numeric) * COALESCE(i.quantity::int, 0)) AS revenue
        FROM orders o
        JOIN order_items i ON i.order_id = o.id
        JOIN products p ON p.id = i.product_id
        JOIN companies c ON c.id = o.company_id
        WHERE ${where.join(' AND ')}
          AND i.unit_price IS NOT NULL
      ),
      agg AS (
        SELECT
          day,
          SUM(quantity)::int AS qty,
          (SUM(revenue) / NULLIF(SUM(quantity), 0))::numeric AS avg_price
        FROM base
        GROUP BY 1
      )
      SELECT
        to_char(d.day, 'YYYY-MM-DD') AS ymd,
        to_char(d.day, 'DD/MM') AS date,
        COALESCE(a.qty, 0)::int AS qty,
        a.avg_price AS avg_price
      FROM days d
      LEFT JOIN agg a ON a.day = d.day
      ORDER BY d.day ASC
    `;

    const rows = await AppDataSource.query(sql, params);
    return res.json({
      start,
      end,
      sku,
      rows: (rows || []).map((r: any) => ({
        ymd: String(r.ymd ?? ''),
        date: String(r.date ?? ''),
        qty: Number(r.qty ?? 0) || 0,
        avgPrice: r.avg_price === null ? null : String(r.avg_price),
      })),
    });
  } catch (err: any) {
    return res.status(500).json({ message: err?.message || 'Erro ao carregar série do SKU' });
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

  const extractCityState = (addr: any): { city: string | null; state: string | null } => {
    const obj = addr && typeof addr === 'object' ? addr : null;
    if (!obj) return { city: null, state: null };
    const cityRaw =
      (obj as any).city ??
      (obj as any).cidade ??
      (obj as any).locality ??
      (obj as any).municipio ??
      (obj as any).municipality ??
      null;
    const stateRaw = (obj as any).state ?? (obj as any).uf ?? (obj as any).estado ?? (obj as any).province ?? null;
    const city = cityRaw ? String(cityRaw).trim() : null;
    const state = stateRaw ? String(stateRaw).trim() : null;
    return { city: city || null, state: state || null };
  };

  return res.json(
    rows.map((c) => ({
      id: c.id,
      tax_id: c.tax_id,
      legal_name: c.legal_name ?? null,
      trade_name: c.trade_name ?? null,
      email: c.email ?? null,
      city: extractCityState(c.delivery_address as any).city,
      state: extractCityState(c.delivery_address as any).state,
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
  const start = String((req.query as any)?.start ?? '').trim();
  const end = String((req.query as any)?.end ?? '').trim();
  const day = String((req.query as any)?.day ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 100);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 100;
  const pageRaw = Number((req.query as any)?.page ?? 1);
  const page = Number.isInteger(pageRaw) ? Math.min(Math.max(pageRaw, 1), 100000) : 1;
  const offset = (page - 1) * limit;

  const repo = AppDataSource.getRepository(Orders);
  const qb = repo
    .createQueryBuilder('o')
    .leftJoin(Customers, 'c', 'c.id = o.customer_id AND c.company_id = o.company_id')
    .where('o.company_id = :companyId', { companyId });

  if ((start && !isIsoYmd(start)) || (end && !isIsoYmd(end))) {
    return res.status(400).json({ message: 'Parâmetros inválidos: start/end devem estar em YYYY-MM-DD.' });
  }
  if (day && !isIsoYmd(day)) {
    return res.status(400).json({ message: 'Parâmetro inválido: day deve estar em YYYY-MM-DD.' });
  }

  // Filtro simples por dia (preferido para o front); mantém start/end por compat.
  if (day) {
    qb.andWhere('o.order_date IS NOT NULL');
    qb.andWhere('o.order_date::date = :day', { day });
  } else if (start && end) {
    qb.andWhere('o.order_date IS NOT NULL');
    qb.andWhere('o.order_date::date BETWEEN :start AND :end', { start, end });
  } else if (start) {
    qb.andWhere('o.order_date IS NOT NULL');
    qb.andWhere('o.order_date::date >= :start', { start });
  } else if (end) {
    qb.andWhere('o.order_date IS NOT NULL');
    qb.andWhere('o.order_date::date <= :end', { end });
  }

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
      'o.channel AS channel',
      'o.customer_id AS customer_id',
      "COALESCE(NULLIF(TRIM(c.trade_name), ''), NULLIF(TRIM(c.legal_name), '')) AS customer_name",
      'c.email AS customer_email',
      'c.tax_id AS customer_tax_id',
    ])
    .orderBy('o.id', 'DESC')
    .offset(offset)
    .limit(limit + 1)
    .getRawMany<any>();

  const mapped = rows.slice(0, limit).map((r) => ({
      id: Number(r.id),
      order_code: Number(r.order_code),
      order_date: r.order_date ?? null,
      current_status: r.current_status ?? null,
      total_amount: r.total_amount ?? null,
      marketplace_name: r.marketplace_name ?? null,
      channel: r.channel ?? null,
      customer: r.customer_id
        ? {
            id: Number(r.customer_id),
            name: r.customer_name ?? null,
            email: r.customer_email ?? null,
            tax_id: r.customer_tax_id ?? null,
          }
        : null,
    }));
  const hasMore = rows.length > limit;

  return res.json({ page, limit, hasMore, rows: mapped });
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
  const field = String((req.query as any)?.field ?? '').trim().toLowerCase();
  const value = String((req.query as any)?.value ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  const repo = AppDataSource.getRepository(Products);
  const qb = repo.createQueryBuilder('p').where('p.company_id = :companyId', { companyId });

  const allowedFields = new Set(['sku', 'brand', 'category', 'subcategory', 'final_category']);
  if (field && value && allowedFields.has(field)) {
    const v = `%${value}%`;
    if (field === 'sku') qb.andWhere(`CAST(p.sku AS text) ILIKE :v`, { v });
    else if (field === 'brand') qb.andWhere(`p.brand ILIKE :v`, { v });
    else if (field === 'category') qb.andWhere(`p.category ILIKE :v`, { v });
    else if (field === 'subcategory') qb.andWhere(`p.subcategory ILIKE :v`, { v });
    else if (field === 'final_category') qb.andWhere(`p.final_category ILIKE :v`, { v });
  } else if (q) {
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
      category: (p as any).category ?? null,
      subcategory: (p as any).subcategory ?? null,
      final_category: (p as any).final_category ?? null,
      photo: p.photo ?? null,
      url: p.url ?? null,
    })),
  );
});

companiesRouter.get('/me/products/distinct-values', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const rows = await AppDataSource.query(
    `
    SELECT
      ARRAY(
        SELECT DISTINCT TRIM(p.brand)
        FROM products p
        WHERE p.company_id = $1 AND p.brand IS NOT NULL AND TRIM(p.brand) <> ''
        ORDER BY 1
        LIMIT 500
      ) AS brands,
      ARRAY(
        SELECT DISTINCT TRIM(p.model)
        FROM products p
        WHERE p.company_id = $1 AND p.model IS NOT NULL AND TRIM(p.model) <> ''
        ORDER BY 1
        LIMIT 500
      ) AS models,
      ARRAY(
        SELECT DISTINCT TRIM(p.category)
        FROM products p
        WHERE p.company_id = $1 AND p.category IS NOT NULL AND TRIM(p.category) <> ''
        ORDER BY 1
        LIMIT 500
      ) AS categories,
      ARRAY(
        SELECT DISTINCT TRIM(p.subcategory)
        FROM products p
        WHERE p.company_id = $1 AND p.subcategory IS NOT NULL AND TRIM(p.subcategory) <> ''
        ORDER BY 1
        LIMIT 700
      ) AS subcategories,
      ARRAY(
        SELECT DISTINCT TRIM(p.final_category)
        FROM products p
        WHERE p.company_id = $1 AND p.final_category IS NOT NULL AND TRIM(p.final_category) <> ''
        ORDER BY 1
        LIMIT 900
      ) AS final_categories
    `,
    [companyId],
  );

  const r = rows?.[0] || {};
  return res.json({
    brands: r.brands ?? [],
    models: r.models ?? [],
    categories: r.categories ?? [],
    subcategories: r.subcategories ?? [],
    finalCategories: r.final_categories ?? [],
  });
});

companiesRouter.get('/me/products/marketplace-status', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const q = String((req.query as any)?.q ?? '').trim();
  const field = String((req.query as any)?.field ?? '').trim().toLowerCase();
  const value = String((req.query as any)?.value ?? '').trim();
  const limitRaw = Number((req.query as any)?.limit ?? 50);
  const limit = Number.isInteger(limitRaw) ? Math.min(Math.max(limitRaw, 1), 200) : 50;

  const repo = AppDataSource.getRepository(Products);
  const qb = repo.createQueryBuilder('p').where('p.company_id = :companyId', { companyId });

  const allowedFields = new Set(['sku', 'brand', 'category', 'subcategory', 'final_category']);
  if (field && value && allowedFields.has(field)) {
    const v = `%${value}%`;
    if (field === 'sku') qb.andWhere(`CAST(p.sku AS text) ILIKE :v`, { v });
    else if (field === 'brand') qb.andWhere(`p.brand ILIKE :v`, { v });
    else if (field === 'category') qb.andWhere(`p.category ILIKE :v`, { v });
    else if (field === 'subcategory') qb.andWhere(`p.subcategory ILIKE :v`, { v });
    else if (field === 'final_category') qb.andWhere(`p.final_category ILIKE :v`, { v });
  } else if (q) {
    qb.andWhere(
      `(CAST(p.sku AS text) ILIKE :q OR p.name ILIKE :q OR p.ean ILIKE :q OR p.slug ILIKE :q OR p.external_reference ILIKE :q OR p.store_reference ILIKE :q)`,
      { q: `%${q}%` },
    );
  }

  const products = await qb.orderBy('p.id', 'DESC').limit(limit).getMany();
  const productIds = products.map((p) => p.id).filter((id) => Number.isInteger(id) && id > 0);

  if (!productIds.length) return res.json({ marketplaces: [], rows: [] });

  const marketplacesRows = await AppDataSource.query(
    `
    SELECT DISTINCT
      COALESCE(NULLIF(TRIM(o.marketplace_name), ''), NULLIF(TRIM(o.channel), '')) AS marketplace
    FROM orders o
    WHERE o.company_id = $1
      AND COALESCE(NULLIF(TRIM(o.marketplace_name), ''), NULLIF(TRIM(o.channel), '')) IS NOT NULL
    ORDER BY 1
    LIMIT 50
    `,
    [companyId],
  );
  const marketplaces = (marketplacesRows || [])
    .map((r: any) => String(r.marketplace ?? '').trim())
    .filter((s: string) => s.length > 0);

  const statusRows = await AppDataSource.query(
    `
    SELECT
      i.product_id::int AS product_id,
      COALESCE(NULLIF(TRIM(o.marketplace_name), ''), NULLIF(TRIM(o.channel), '')) AS marketplace,
      ((now() AT TIME ZONE 'America/Sao_Paulo')::date - MAX(o.order_date::date))::int AS days_without_sales
    FROM orders o
    JOIN order_items i ON i.order_id = o.id
    WHERE o.company_id = $1
      AND i.product_id = ANY($2::int[])
      AND COALESCE(NULLIF(TRIM(o.marketplace_name), ''), NULLIF(TRIM(o.channel), '')) IS NOT NULL
    GROUP BY i.product_id, marketplace
    `,
    [companyId, productIds],
  );

  const byProduct = new Map<number, Record<string, number>>();
  for (const r of statusRows || []) {
    const pid = Number(r.product_id ?? 0) || 0;
    const mk = String(r.marketplace ?? '').trim();
    const days = Number(r.days_without_sales ?? null);
    if (!pid || !mk || !Number.isFinite(days)) continue;
    const cur = byProduct.get(pid) ?? {};
    cur[mk] = days;
    byProduct.set(pid, cur);
  }

  return res.json({
    marketplaces,
    rows: products.map((p) => ({
      id: p.id,
      sku: p.sku,
      name: p.name ?? null,
      photo: p.photo ?? null,
      status: byProduct.get(p.id) ?? {},
    })),
  });
});

companiesRouter.post('/me/products/classify-ai', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const key = String(process.env.OPENAI_API_KEY ?? '').trim();
  if (!key) return res.status(500).json({ message: 'OPENAI_API_KEY não configurada no servidor.' });

  const body = (req.body ?? {}) as any;
  const idsRaw = Array.isArray(body?.ids) ? body.ids : [];
  const ids = idsRaw
    .map((x: any) => Number(x))
    .filter((n: number) => Number.isInteger(n) && n > 0);
  if (!ids.length) return res.status(400).json({ message: 'ids obrigatório (lista de inteiros positivos).' });

  const prompt = loadPromptFile(path.join('prompt', 'classificar.md'));

  // base de categorias existentes (cap para não explodir tokens)
  const existing = await AppDataSource.query(
    `
    SELECT
      ARRAY(SELECT DISTINCT TRIM(p.category) FROM products p WHERE p.company_id = $1 AND p.category IS NOT NULL AND TRIM(p.category) <> '' ORDER BY 1 LIMIT 300) AS categories,
      ARRAY(SELECT DISTINCT TRIM(p.subcategory) FROM products p WHERE p.company_id = $1 AND p.subcategory IS NOT NULL AND TRIM(p.subcategory) <> '' ORDER BY 1 LIMIT 400) AS subcategories,
      ARRAY(SELECT DISTINCT TRIM(p.final_category) FROM products p WHERE p.company_id = $1 AND p.final_category IS NOT NULL AND TRIM(p.final_category) <> '' ORDER BY 1 LIMIT 600) AS final_categories
    `,
    [companyId],
  );
  const existingObj = existing?.[0] || {};

  const candidates = await AppDataSource.getRepository(Products)
    .createQueryBuilder('p')
    .where('p.company_id = :companyId', { companyId })
    .andWhere('p.id = ANY(:ids)', { ids })
    .andWhere(
      `(
        p.category IS NULL OR TRIM(p.category) = '' OR
        p.subcategory IS NULL OR TRIM(p.subcategory) = '' OR
        p.final_category IS NULL OR TRIM(p.final_category) = ''
      )`,
    )
    .orderBy('p.id', 'DESC')
    .getMany();
  if (!candidates.length) {
    return res.json({
      ok: true,
      processed: 0,
      updated: 0,
      batches: 0,
      message: 'Nenhum dos itens selecionados precisa de classificação (categoria/subcategoria/categoria final já preenchidos).',
    });
  }

  type ItemIn = {
    sku: string;
    name: string;
    categoria: string | null;
    subcategoria: string | null;
    categoria_final: string | null;
  };

  const itemsAll: ItemIn[] = candidates.map((p) => ({
    sku: String(p.sku ?? ''),
    name: String(p.name ?? ''),
    categoria: safeTrimOrNull((p as any).category),
    subcategoria: safeTrimOrNull((p as any).subcategory),
    categoria_final: safeTrimOrNull((p as any).final_category),
  }));

  const productBySku = new Map<string, Products>();
  for (const p of candidates) productBySku.set(String(p.sku ?? ''), p);

  const BATCH = 20;
  const batches: ItemIn[][] = [];
  for (let i = 0; i < itemsAll.length; i += BATCH) batches.push(itemsAll.slice(i, i + BATCH));

  let updated = 0;
  const errors: any[] = [];

  for (let bi = 0; bi < batches.length; bi++) {
    const batch = batches[bi];
    const userPayload = {
      existing: {
        categories: existingObj.categories ?? [],
        subcategories: existingObj.subcategories ?? [],
        final_categories: existingObj.final_categories ?? [],
      },
      items: batch,
    };

    const openaiResp = await fetch('https://api.openai.com/v1/responses', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${key}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        model: 'gpt-4.1',
        temperature: 0.2,
        max_output_tokens: 2500,
        input: [
          { role: 'system', content: prompt },
          {
            role: 'user',
            content:
              `Classifique os itens abaixo.\n` +
              `Responda SOMENTE com um JSON (sem markdown) no formato:\n` +
              `{"items":[{"sku","name","categoria","subcategoria","categoria_final"}]}\n` +
              `- Não altere sku nem name.\n` +
              `- Só preencha campos que estiverem null/vazio.\n\n` +
              JSON.stringify(userPayload),
          },
        ],
      }),
    });

    const raw = await openaiResp.json().catch(() => ({}));
    if (!openaiResp.ok) {
      errors.push({ batch: bi, message: 'Erro OpenAI', status: openaiResp.status, payload: raw });
      continue;
    }

    const text = extractOpenAIText(raw);
    let parsed: any = null;
    try {
      parsed = extractJsonFromText(text);
    } catch (e: any) {
      errors.push({ batch: bi, message: 'Resposta não é JSON válido', detail: String(e?.message || e), text: String(text).slice(0, 1200) });
      continue;
    }

    const outItems = Array.isArray((parsed as any)?.items) ? (parsed as any).items : Array.isArray(parsed) ? parsed : null;
    if (!outItems) {
      errors.push({ batch: bi, message: 'JSON sem items (formato inesperado)', parsed });
      continue;
    }

    const outMap = new Map<string, any>();
    for (const it of outItems) {
      const sku = safeTrimOrNull(it?.sku);
      if (sku) outMap.set(sku, it);
    }

    for (const original of batch) {
      const it = outMap.get(original.sku);
      if (!it) continue;
      const nextCategoria = safeTrimOrNull(it?.categoria);
      const nextSub = safeTrimOrNull(it?.subcategoria);
      const nextFinal = safeTrimOrNull(it?.categoria_final);

      // aplica apenas em campos vazios
      const updateSet: any = {};
      const current = productBySku.get(original.sku);
      if (!current) continue;

      const curCat = safeTrimOrNull((current as any).category);
      const curSub = safeTrimOrNull((current as any).subcategory);
      const curFinal = safeTrimOrNull((current as any).final_category);

      if (!curCat && nextCategoria) updateSet.category = nextCategoria;
      if (!curSub && nextSub) updateSet.subcategory = nextSub;
      if (!curFinal && nextFinal) updateSet.final_category = nextFinal;

      const hasAny = Object.keys(updateSet).length > 0;
      if (!hasAny) continue;

      updateSet.manual_attributes_locked = true;

      const result = await AppDataSource.createQueryBuilder()
        .update(Products)
        .set(updateSet)
        .where('company_id = :companyId', { companyId })
        .andWhere('id = :id', { id: current.id })
        .execute();

      if ((result.affected ?? 0) > 0) updated += 1;
    }
  }

  return res.json({
    ok: true,
    processed: candidates.length,
    updated,
    batches: batches.length,
    errors,
  });
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

companiesRouter.put('/me/products/bulk-update', async (req: Request, res: Response) => {
  const userId = await getAuthUserId(req);
  if (!userId) return res.status(401).json({ message: 'Não autenticado' });
  const companyId = await getAuthCompanyId(req);
  if (!companyId) return res.status(400).json({ message: 'Company not configured for this user' });

  const body = (req.body ?? {}) as any;
  const idsRaw = Array.isArray(body.ids) ? body.ids : [];
  const ids = idsRaw
    .map((x: any) => Number(x))
    .filter((n: number) => Number.isInteger(n) && n > 0);
  if (!ids.length) return res.status(400).json({ message: 'ids obrigatório (lista de inteiros positivos).' });

  const fields = (body.fields ?? {}) as Record<string, any>;
  const next: Partial<Products> & Record<string, any> = {};

  const pickNullableString = (v: any): string | null | undefined => {
    if (v === undefined) return undefined;
    if (v === null) return null;
    const s = String(v).trim();
    return s ? s : null;
  };

  const brand = pickNullableString(fields.brand);
  const model = pickNullableString(fields.model);
  const category = pickNullableString(fields.category);
  const subcategory = pickNullableString(fields.subcategory);
  const finalCategory = pickNullableString(fields.final_category);

  if (brand !== undefined) next.brand = brand;
  if (model !== undefined) next.model = model;
  if (category !== undefined) next.category = category;
  if (subcategory !== undefined) next.subcategory = subcategory;
  if (finalCategory !== undefined) next.final_category = finalCategory;

  const lock = body.lock === undefined ? true : Boolean(body.lock);
  if (lock) next.manual_attributes_locked = true;

  const hasAnyField =
    brand !== undefined || model !== undefined || category !== undefined || subcategory !== undefined || finalCategory !== undefined;
  if (!hasAnyField && !lock) return res.status(400).json({ message: 'Nenhuma alteração informada.' });

  const result = await AppDataSource.createQueryBuilder()
    .update(Products)
    .set(next)
    .where('company_id = :companyId', { companyId })
    .andWhere('id = ANY(:ids)', { ids })
    .execute();

  return res.json({ ok: true, affected: result.affected ?? 0 });
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
