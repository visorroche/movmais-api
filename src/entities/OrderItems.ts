import { Column, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm';
import { Companies } from './Companies';
import { Orders } from './Orders';
import { Products } from './Products';

@Entity('order_items')
export class OrderItems {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'int' })
  company_id!: number;

  @ManyToOne(() => Companies, { nullable: false })
  @JoinColumn({ name: 'company_id' })
  company!: Companies;

  @Column({ type: 'int' })
  order_id!: number;

  @ManyToOne(() => Orders, (order: Orders) => order.items, { nullable: false, onDelete: 'CASCADE' })
  @JoinColumn({ name: 'order_id' })
  order!: Orders;

  @Column({ type: 'int', nullable: true })
  product_id?: number | null;

  @ManyToOne(() => Products, { nullable: true })
  @JoinColumn({ name: 'product_id' })
  product?: Products | null;

  @Column({ type: 'int', nullable: true })
  sku?: number | null;

  @Column({ type: 'numeric', precision: 14, scale: 2, nullable: true })
  unit_price?: string | null;

  @Column({ type: 'numeric', precision: 14, scale: 2, nullable: true })
  net_unit_price?: string | null;

  @Column({ type: 'int', nullable: true })
  quantity?: number | null;

  @Column({ type: 'varchar', nullable: true })
  item_type?: string | null;

  @Column({ type: 'varchar', nullable: true })
  service_ref_sku?: string | null;
}


