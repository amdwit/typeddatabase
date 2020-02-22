import { Client, QueryResult, types } from "pg";
import { Db, IdBase, Row, TableName } from './db';

types.setTypeParser(1114, (stringValue:string) => {
  return new Date(stringValue + "Z");
});

types.setTypeParser(1082, (stringValue:string) => {
  return stringValue ;
});

interface CreateOptions {
  id: boolean;
}

const checkName = (name: string): string => {
  if (/^[a-zA-Z_][a-zA-Z_\d]{0,30}$/.test(name)) {
    return name;
  } else {
    throw new Error("illegal name: " + name);
  }
};

interface SplitUpResult {
  columns: string[];
  variables: string[];
  params: any[];
  assignments: string[];
  whereSection: string;
}

const splitUp = (
  fields: { [key: string]: any },
  isWhere: boolean = false,
  offset: number = 0
): SplitUpResult => {
  const fieldsArray = Object.entries(fields);
  const result = fieldsArray.reduce<SplitUpResult>(
    (result, [key, value]) => {
      const column = `"${checkName(key)}"`;
      if (isWhere && value === null) {
        result.assignments.push(`${column} is null`);
      } else {
        result.columns.push(column);
        const variable = `$${offset + result.variables.length + 1}`;
        result.variables.push(variable);
        result.params.push(value);
        result.assignments.push(`${column} = ${variable}`);
      }
      return result;
    },
    {
      columns: [],
      variables: [],
      params: [],
      assignments: [],
      whereSection: ""
    }
  );
  if (isWhere) {
    result.whereSection = result.assignments.length
      ? ` where ${result.assignments.join(" and ")}`
      : "";
  }
  return result;
};

export class PostgreSqlDb<Database,Id extends IdBase> implements Db<Database,Id> {
  private client: Client;

  private constructor(config: any) {
    this.client = new Client(config);
  }

  static async create<Database,Id extends IdBase>(config: any): Promise<PostgreSqlDb<Database,Id>> {
    const instance = new PostgreSqlDb<Database,Id>(config);
    await instance.connect();
    return instance;
  }

  async end() {
    return this.client.end();
  }

  async writeRows<T extends TableName<Database>, R extends Database[T]>(
    tableName: T,
    rows: R[],
    unique: Partial<keyof R>[]
  ): Promise<void> {
    await this.query("begin");
    await this.query("SET CONSTRAINTS fk_parent DEFERRED");
    for (const row of rows) {
      await this.upsert<R>(tableName, row as any, unique);
    }
    await this.query("commit");
  }

  async deleteRows<T extends TableName<Database>>(
    tableName: T,
    ids: Id[]
  ): Promise<void> {
    await this.query("begin");
    await this.query("SET CONSTRAINTS fk_parent DEFERRED");
    for (const id of ids) {
      await this.delete(tableName, { id });
    }
    await this.query("commit");
  }

  async getAll<T extends TableName<Database>>(
    tableName: T,
    where: Partial<Database[T]> = {}
  ): Promise<Database[T][]> {
    return this.select(tableName, where);
  }

  async updateById<Ta extends TableName<Database>>(
    tableName: Ta,
    id: Id,
    fields: Omit<Partial<Database[Ta]>, "id">
  ): Promise<void> {
    await this.update<Database[Ta]>(
      tableName,
      fields as any,
      { id } as any
    );
  }

  async getItemById<Ta extends TableName<Database>>(
    tableName: Ta,
    id: Id
  ): Promise<Database[Ta]> {
    const rows = await this.select<Database[Ta]>(tableName, {
      id
    } as any);
    return rows[0];
  }

  async query<R>(sql: string, params?: any[]): Promise<QueryResult<R>> {
    const newParams = params
      ? params.map(param => {
        if (typeof param === "object" && param instanceof Date) {
          return param.toISOString();
        } else {
          return param;
        }
      })
      : undefined;
    return this.client.query<R>(sql, newParams);
  }

  private async connect() {
    await this.client.connect();
  }

  private query2 = async <T>(sql: string, params: any[]): Promise<T[]> => {
    try {
      const queryResult = await this.query<T>(sql, params);
      return queryResult.rows;
    } catch (e) {
      console.error(e);
      throw e;
    }
  };

  create = async <T>(
    table: string,
    fields: Partial<T>
  ): Promise<T[]> => {
    const { columns, variables, params } = splitUp(fields);
    const sql = `insert into "${checkName(
      table
    )}" (${columns}) values (${variables})`;
    return this.query2<T>(sql, params);
  };

  upsert = async <T>(
    table: string,
    fields: Partial<T>,
    unique: Partial<keyof T>[]
  ): Promise<T[]> => {
    const { columns, variables, params, assignments } = splitUp(fields);
    const sql = `insert into "${checkName(
      table
    )}" (${columns}) values (${variables}) on conflict (${unique}) do update set ${assignments}`;
    return this.query2<T>(sql, params);
  };

  update = async <T>(
    table: string,
    fields: Partial<T>,
    whereConditions: Partial<T>
  ): Promise<T[]> => {
    const { params, assignments } = splitUp(fields);
    const { params: whereParams, whereSection } = splitUp(
      whereConditions,
      true,
      params.length
    );
    const sql = `update "${checkName(
      table
    )}" set ${assignments}${whereSection}`;
    return this.query2(sql, [...params, ...whereParams]);
  };

  select = async <T>(
    table: string,
    whereConditions: Partial<T>
  ): Promise<T[]> => {
    const { params: whereParams, whereSection } = splitUp(
      whereConditions,
      true
    );
    const sql = `select * from "${checkName(table)}"${whereSection}`;

    const rows = await this.query2<T>(sql, whereParams);
    return rows;
  };

  delete = async <T extends Row>(
    table: string,
    whereConditions: Partial<T>
  ): Promise<T[]> => {
    const { params: whereParams, whereSection } = splitUp(
      whereConditions,
      true
    );
    const sql = `delete from "${checkName(table)}"${whereSection}`;
    return this.query2(sql, whereParams);
  };
}
