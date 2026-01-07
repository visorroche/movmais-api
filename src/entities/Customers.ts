import { Column, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn, Unique } from 'typeorm';
import { Companies } from './Companies';

@Entity('customers')
@Unique('UQ_customers_company_id_external_id', ['company_id', 'external_id'])
export class Customers {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'varchar' })
  tax_id!: string;

  @Column({ type: 'varchar', nullable: true })
  state_registration?: string | null;

  @Column({ type: 'varchar', nullable: true })
  person_type?: string | null;

  @Column({ type: 'varchar', nullable: true })
  legal_name?: string | null;

  @Column({ type: 'varchar', nullable: true })
  trade_name?: string | null;

  @Column({ type: 'varchar', nullable: true })
  gender?: string | null;

  @Column({ type: 'date', nullable: true })
  birth_date?: Date | null;

  @Column({ type: 'varchar', nullable: true })
  email?: string | null;

  @Column({ type: 'varchar', nullable: true })
  status?: string | null;

  @Column({ type: 'jsonb', nullable: true })
  delivery_address?: unknown;

  @Column({ type: 'jsonb', nullable: true })
  phones?: unknown;

  @Column({ type: 'varchar', nullable: true })
  external_id?: string | null;

  @Column({ type: 'int' })
  company_id!: number;

  @ManyToOne(() => Companies, (company: Companies) => company.customers, { nullable: false })
  @JoinColumn({ name: 'company_id' })
  company!: Companies;

  @Column({ type: 'jsonb', nullable: true })
  raw?: unknown;
}


