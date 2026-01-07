import { Column, Entity, OneToMany, PrimaryGeneratedColumn } from 'typeorm';
import { CompanyPlatforms } from './CompanyPlatforms';

@Entity('platforms')
export class Platforms {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'varchar' })
  type!: string;

  @Column({ type: 'varchar', unique: true })
  slug!: string;

  @Column({ type: 'varchar' })
  name!: string;

  @Column({ type: 'jsonb' })
  parameters!: unknown;

  @OneToMany(() => CompanyPlatforms, (cp: CompanyPlatforms) => cp.platform)
  company_platforms?: CompanyPlatforms[];
}


