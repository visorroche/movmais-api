import { Column, CreateDateColumn, DeleteDateColumn, Entity, OneToMany, PrimaryGeneratedColumn, UpdateDateColumn } from 'typeorm';
import { Users } from './Users';

@Entity('companies')
export class Companies {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'text' })
  name!: string;

  @Column({ type: 'text', nullable: true })
  document?: string;

  @Column({ type: 'text', nullable: true })
  commercial_email?: string;

  @Column({ type: 'text', nullable: true })
  commercial_phone?: string;

  @Column({ type: 'text', nullable: true })
  site?: string;

  @Column({ type: 'text', nullable: true })
  logo?: string;

  @Column({ type: 'text', nullable: true })
  highlight_color?: string;

  @Column({ type: 'text', nullable: true })
  presentation_text?: string;

  @Column({ type: 'timestamp', nullable: true })
  assigned_at?: Date;

  @Column({ type: 'timestamp', nullable: true })
  canceled_at?: Date;

  @Column({ type: 'int', nullable: true })
  owner_id?: number;

  @OneToMany(() => Users, (user: Users) => user.company)
  users!: Users[];

  @CreateDateColumn({ type: 'timestamp' })
  created_at!: Date;

  @UpdateDateColumn({ type: 'timestamp', nullable: true })
  updated_at?: Date;

  @DeleteDateColumn({ type: 'timestamp', nullable: true })
  deleted_at?: Date;
}
