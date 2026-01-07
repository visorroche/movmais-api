import { Entity, JoinColumn, ManyToOne, PrimaryGeneratedColumn, Column } from 'typeorm';
import { Companies } from './Companies';
import { Users } from './Users';

@Entity('company_users')
export class CompanyUsers {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'boolean', default: false })
  owner!: boolean;

  @Column({ type: 'int', nullable: true })
  company_id?: number | null;

  @Column({ type: 'int', nullable: true })
  user_id?: number | null;

  @ManyToOne(() => Companies, (company: Companies) => company.company_users, { nullable: true })
  @JoinColumn({ name: 'company_id' })
  company?: Companies | null;

  @ManyToOne(() => Users, (user: Users) => user.company_users, { nullable: true })
  @JoinColumn({ name: 'user_id' })
  user?: Users | null;
}


