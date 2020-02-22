
export type DatabaseBase<T extends string>={[table in T]:object}

export type IdBase=string|number

export type Row<Id extends IdBase|Symbol=Symbol> = {
  [key: string]: number | string | boolean | null | Date;
}&(Id extends IdBase ? {id:Id}:{});

export type TableName<Database>=Extract<keyof Database, string>

export abstract class Db<Database, Id> {
  abstract async writeRows<T extends TableName<Database>, R extends Database[T]>(
    tableName: T,
    rows: R[],
    unique: Partial<keyof R>[]
  ): Promise<void>;

  abstract async deleteRows<T extends TableName<Database>>(
    tableName: T,
    ids: Id[]
  ): Promise<void>;

  abstract async getAll<T extends TableName<Database>>(
    tableName: T,
    where?: Partial<Database[T]>
  ): Promise<Database[T][]>;

  abstract async updateById<T extends TableName<Database>>(
    tableName: T,
    id: Id,
    fields: Omit<Partial<Database[T]>, "id">
  ): Promise<void>;

  abstract async getItemById<T extends TableName<Database>>(
    tableName: T,
    id: Id
  ): Promise<Database[T]>;
}

