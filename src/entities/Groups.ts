import { Column, Entity, OneToMany, PrimaryGeneratedColumn } from 'typeorm';
import { Companies } from './Companies';

@Entity('groups')
export class Groups {
  @PrimaryGeneratedColumn('increment')
  id!: number;

  @Column({ type: 'varchar' })
  name!: string;

  @OneToMany(() => Companies, (company: Companies) => company.group)
  companies?: Companies[];
}


