import { Column, Entity, OneToMany, PrimaryGeneratedColumn } from 'typeorm';
import { CompanyUsers } from './CompanyUsers';

export type UserType = 'admin' | 'user';

@Entity('users')
export class Users {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'varchar' })
  name!: string;

  // Mapeia para o enum já existente no Postgres: public.users_type_enum
  @Column({ type: 'enum', enum: ['admin', 'user'], enumName: 'users_type_enum' })
  type!: UserType;

  @Column({ type: 'varchar', unique: true })
  email!: string;

  @Column({ type: 'varchar' })
  password!: string;

  @OneToMany(() => CompanyUsers, (cu: CompanyUsers) => cu.user)
  company_users?: CompanyUsers[];
}
