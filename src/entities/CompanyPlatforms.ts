import { Column, Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn, Unique } from 'typeorm';
import { Companies } from './Companies';
import { Platforms } from './Platforms';

@Entity('company_platforms')
@Unique('UQ_company_platforms_company_id_platform_id', ['company_id', 'platform_id'])
export class CompanyPlatforms {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'jsonb' })
  config!: unknown;

  @Column({ type: 'int', nullable: true })
  company_id?: number | null;

  @Column({ type: 'int', nullable: true })
  platform_id?: number | null;

  @ManyToOne(() => Companies, (company: Companies) => company.company_platforms, { nullable: true })
  @JoinColumn({ name: 'company_id' })
  company?: Companies | null;

  @ManyToOne(() => Platforms, (platform: Platforms) => platform.company_platforms, { nullable: true })
  @JoinColumn({ name: 'platform_id' })
  platform?: Platforms | null;
}


