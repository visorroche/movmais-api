import { Column, Entity, JoinColumn, ManyToOne, OneToMany, PrimaryGeneratedColumn } from 'typeorm';
import { Groups } from './Groups';
import { CompanyUsers } from './CompanyUsers';
import { CompanyPlatforms } from './CompanyPlatforms';
import { Customers } from './Customers';

@Entity('companies')
export class Companies {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'int', nullable: true })
  group_id?: number | null;

  @ManyToOne(() => Groups, (group: Groups) => group.companies, { nullable: true })
  @JoinColumn({ name: 'group_id' })
  group?: Groups | null;

  @Column({ type: 'varchar', default: 'Sem nome' })
  name!: string;

  @Column({ type: 'varchar', default: '' })
  site!: string;

  @OneToMany(() => CompanyPlatforms, (cp: CompanyPlatforms) => cp.company)
  company_platforms?: CompanyPlatforms[];

  @OneToMany(() => CompanyUsers, (cu: CompanyUsers) => cu.company)
  company_users?: CompanyUsers[];

  @OneToMany(() => Customers, (c: Customers) => c.company)
  customers?: Customers[];
}
