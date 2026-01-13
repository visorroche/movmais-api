import { Column, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn, Unique } from 'typeorm';
import { Companies } from './Companies';

@Entity('products')
@Unique('UQ_products_company_id_sku', ['company_id', 'sku'])
export class Products {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'int' })
  company_id!: number;

  @ManyToOne(() => Companies, { nullable: false })
  @JoinColumn({ name: 'company_id' })
  company!: Companies;

  // SKU pode vir com valores grandes (ex.: integrações) — manter como string para evitar overflow de int32 no Postgres.
  @Column({ type: 'varchar' })
  sku!: string;

  @Column({ type: 'int', nullable: true })
  ecommerce_id?: number | null;

  @Column({ type: 'varchar', nullable: true })
  ean?: string | null;

  @Column({ type: 'varchar', nullable: true })
  slug?: string | null;

  @Column({ type: 'varchar', nullable: true })
  name?: string | null;

  @Column({ type: 'varchar', nullable: true })
  store_reference?: string | null;

  @Column({ type: 'varchar', nullable: true })
  external_reference?: string | null;

  @Column({ type: 'int', nullable: true })
  brand_id?: number | null;

  @Column({ type: 'varchar', nullable: true })
  brand?: string | null;

  @Column({ type: 'varchar', nullable: true })
  model?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 3, nullable: true })
  weight?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 3, nullable: true })
  width?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 3, nullable: true })
  height?: string | null;

  // OBS: nome da coluna no banco é "lenght"
  @Column({ name: 'lenght', type: 'numeric', precision: 14, scale: 3, nullable: true })
  length_cm?: string | null;

  @Column({ type: 'varchar', nullable: true })
  ncm?: string | null;

  @Column({ type: 'varchar', nullable: true })
  category?: string | null;

  @Column({ type: 'int', nullable: true })
  category_id?: number | null;

  @Column({ type: 'varchar', nullable: true })
  subcategory?: string | null;

  @Column({ type: 'varchar', nullable: true })
  final_category?: string | null;

  @Column({ type: 'varchar', nullable: true })
  photo?: string | null;

  @Column({ type: 'varchar', nullable: true })
  url?: string | null;

  @Column({ type: 'jsonb', nullable: true })
  raw?: unknown;
}


