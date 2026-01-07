import { Column, Entity, JoinColumn, ManyToOne, OneToMany, PrimaryGeneratedColumn, Unique } from 'typeorm';
import { Customers } from './Customers';
import { Companies } from './Companies';
import { Platforms } from './Platforms';
import { OrderItems } from './OrderItems';

@Entity('orders')
@Unique('UQ_orders_company_id_order_code', ['company_id', 'order_code'])
export class Orders {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'int' })
  order_code!: number;

  // No banco: order_date timestamp NULL
  @Column({ type: 'timestamp', nullable: true })
  order_date?: Date | null;

  @Column({ type: 'varchar', nullable: true })
  partner_order_id?: string | null;

  @Column({ type: 'varchar', nullable: true })
  current_status?: string | null;

  @Column({ type: 'varchar', nullable: true })
  current_status_code?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 2, nullable: true })
  shipping_amount?: string | null;

  @Column({ type: 'int', nullable: true })
  delivery_days?: number | null;

  @Column({ type: 'date', nullable: true })
  delivery_date?: Date | null;

  @Column({ type: 'numeric', precision: 14, scale: 2, nullable: true })
  total_amount?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 2, nullable: true })
  total_discount?: string | null;

  @Column({ type: 'varchar', nullable: true })
  marketplace_name?: string | null;

  @Column({ type: 'varchar', nullable: true })
  channel?: string | null;

  @Column({ type: 'date', nullable: true })
  payment_date?: Date | null;

  @Column({ type: 'varchar', nullable: true })
  discount_coupon?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_state?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_zip?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_neighborhood?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_city?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_number?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_address?: string | null;

  @Column({ type: 'varchar', nullable: true })
  delivery_complement?: string | null;

  @Column({ type: 'jsonb', nullable: true })
  metadata?: unknown;

  @Column({ type: 'jsonb', nullable: true })
  store_pickup?: unknown;

  @Column({ type: 'jsonb', nullable: true })
  payments?: unknown;

  @Column({ type: 'jsonb', nullable: true })
  tracking?: unknown;

  @Column({ type: 'jsonb', nullable: true })
  timeline?: unknown;

  @Column({ type: 'jsonb', nullable: true })
  raw?: unknown;

  @Column({ type: 'int', nullable: true })
  customer_id?: number | null;

  @ManyToOne(() => Customers, { nullable: true })
  @JoinColumn({ name: 'customer_id' })
  customer?: Customers | null;

  @Column({ type: 'int' })
  company_id!: number;

  @ManyToOne(() => Companies, { nullable: false })
  @JoinColumn({ name: 'company_id' })
  company!: Companies;

  @Column({ type: 'int', nullable: true })
  platform_id?: number | null;

  @ManyToOne(() => Platforms, { nullable: true })
  @JoinColumn({ name: 'platform_id' })
  platform?: Platforms | null;

  @OneToMany(() => OrderItems, (item: OrderItems) => item.order)
  items?: OrderItems[];
}


