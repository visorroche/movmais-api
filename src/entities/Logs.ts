import { Column, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn } from 'typeorm';
import { Companies } from './Companies';
import { Platforms } from './Platforms';

export type IntegrationCommand = 'Pedidos' | 'Cotações' | 'Produtos';
export type IntegrationStatus = 'PROCESSANDO' | 'FINALIZADO' | 'ERRO';

@Entity('logs')
export class Logs {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  // Timestamp de processamento do job.
  @Column({ name: 'processed_at', type: 'timestamptz', default: () => 'now()' })
  processedAt!: Date;

  // Data usada nos filtros do processamento (quando aplicável; ex.: start-date)
  @Column({ type: 'date', nullable: true })
  date?: Date | null;

  @Column({ type: 'varchar', nullable: true })
  status?: IntegrationStatus | null;

  @Column({ type: 'varchar' })
  command!: IntegrationCommand;

  @Column({ type: 'jsonb' })
  log!: unknown;

  @Column({ type: 'jsonb', nullable: true })
  errors?: unknown | null;

  @Column({ name: 'company_id', type: 'int' })
  companyId!: number;

  @ManyToOne(() => Companies, { nullable: false })
  @JoinColumn({ name: 'company_id' })
  company!: Companies;

  @Column({ name: 'platform_id', type: 'int', nullable: true })
  platformId?: number | null;

  @ManyToOne(() => Platforms, { nullable: true })
  @JoinColumn({ name: 'platform_id' })
  platform?: Platforms | null;
}

