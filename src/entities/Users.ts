import { Column, Entity, OneToMany, PrimaryGeneratedColumn } from 'typeorm';
import { CompanyUsers } from './CompanyUsers';

export type UserType = 'admin' | 'user';

@Entity('users')
export class Users {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'varchar', default: '' })
  name!: string;

  // Mapeia para o enum já existente no Postgres: public.users_type_enum
  @Column({
    type: 'enum',
    enum: ['admin', 'user'],
    enumName: 'users_type_enum',
    default: 'user',
  })
  type!: UserType;

  @Column({ type: 'varchar', unique: true, nullable: true })
  email?: string | null;

  @Column({ type: 'varchar', unique: true, nullable: true })
  phone?: string | null;

  @Column({ type: 'varchar', nullable: true })
  password?: string | null;

  @OneToMany(() => CompanyUsers, (cu: CompanyUsers) => cu.user)
  company_users?: CompanyUsers[];
}
