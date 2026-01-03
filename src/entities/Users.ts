import { Column, Entity, PrimaryGeneratedColumn, CreateDateColumn, UpdateDateColumn, DeleteDateColumn, ManyToOne, JoinColumn } from 'typeorm';
import { Companies } from './Companies';

@Entity('users')
export class Users {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'text', unique: true })
  email!: string;

  @Column({ type: 'text' })
  password!: string;

  @Column({ type: 'text', nullable: true })
  phone?: string;

  @Column({ type: 'text', nullable: true })
  name?: string;

  @Column({ type: 'jsonb', default: () => "'[]'::jsonb" })
  roles!: string[];

  @Column({ type: 'int', nullable: true })
  company_id?: number;

  @ManyToOne(() => Companies, (company: Companies) => company.users, { nullable: true })
  @JoinColumn({ name: 'company_id' })
  company?: Companies;

  @CreateDateColumn({ type: 'timestamp' })
  created_at!: Date;

  @UpdateDateColumn({ type: 'timestamp', nullable: true })
  updated_at?: Date;

  @DeleteDateColumn({ type: 'timestamp', nullable: true })
  deleted_at?: Date;
}
