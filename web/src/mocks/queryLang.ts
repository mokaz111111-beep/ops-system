import { MockApiError } from './errors';

/**
 * 查询串的 mock 侧求值器。
 *
 * IF-007 §2.3 把 `query` 定成不透明字符串、语法未定稿，所以这里实现的只是
 * 「前端 composeQuery 会产出的那一小撮形态」：`path:"value"`、`A AND B`、`(a OR b)`、裸关键字。
 * 它存在的意义是让 mock 能对 facet 点选做出正确反应，不是给语法拍板。
 */

type Node =
  | { kind: 'and'; left: Node; right: Node }
  | { kind: 'or'; left: Node; right: Node }
  | { kind: 'field'; path: string; value: string }
  | { kind: 'text'; value: string };

type Token = { t: 'lparen' | 'rparen' | 'and' | 'or' } | { t: 'word'; v: string; quoted: boolean };

function tokenize(input: string): Token[] {
  const tokens: Token[] = [];
  let i = 0;
  while (i < input.length) {
    const ch = input[i]!;
    if (/\s/.test(ch)) {
      i += 1;
      continue;
    }
    if (ch === '(') {
      tokens.push({ t: 'lparen' });
      i += 1;
      continue;
    }
    if (ch === ')') {
      tokens.push({ t: 'rparen' });
      i += 1;
      continue;
    }
    if (ch === '"') {
      let value = '';
      i += 1;
      while (i < input.length && input[i] !== '"') {
        if (input[i] === '\\' && i + 1 < input.length) i += 1;
        value += input[i]!;
        i += 1;
      }
      if (i >= input.length) {
        throw new MockApiError('QUERY_SYNTAX_ERROR', '引号未闭合', { details: { position: i } });
      }
      i += 1;
      tokens.push({ t: 'word', v: value, quoted: true });
      continue;
    }
    let word = '';
    while (i < input.length && !/[\s()]/.test(input[i]!)) {
      if (input[i] === '"') break;
      word += input[i]!;
      i += 1;
    }
    if (input[i] === '"') {
      // path:"value" 的形态：把引号内容并进当前词
      let value = '';
      i += 1;
      while (i < input.length && input[i] !== '"') {
        if (input[i] === '\\' && i + 1 < input.length) i += 1;
        value += input[i]!;
        i += 1;
      }
      if (i >= input.length) {
        throw new MockApiError('QUERY_SYNTAX_ERROR', '引号未闭合', { details: { position: i } });
      }
      i += 1;
      tokens.push({ t: 'word', v: word + value, quoted: false });
      continue;
    }
    if (word.toUpperCase() === 'AND') tokens.push({ t: 'and' });
    else if (word.toUpperCase() === 'OR') tokens.push({ t: 'or' });
    else tokens.push({ t: 'word', v: word, quoted: false });
  }
  return tokens;
}

function parse(tokens: Token[]): Node {
  let pos = 0;

  const peek = () => tokens[pos];

  function parseExpr(): Node {
    let left = parseOr();
    while (peek()?.t === 'and') {
      pos += 1;
      left = { kind: 'and', left, right: parseOr() };
    }
    return left;
  }

  function parseOr(): Node {
    let left = parseAtom();
    while (peek()?.t === 'or') {
      pos += 1;
      left = { kind: 'or', left, right: parseAtom() };
    }
    return left;
  }

  function parseAtom(): Node {
    const token = peek();
    if (!token) throw new MockApiError('QUERY_SYNTAX_ERROR', '查询串意外结束');
    if (token.t === 'lparen') {
      pos += 1;
      const inner = parseExpr();
      if (peek()?.t !== 'rparen') {
        throw new MockApiError('QUERY_SYNTAX_ERROR', '括号未闭合');
      }
      pos += 1;
      return inner;
    }
    if (token.t !== 'word') {
      throw new MockApiError('QUERY_SYNTAX_ERROR', `意外的记号: ${token.t}`);
    }
    pos += 1;
    if (!token.quoted) {
      const sep = token.v.indexOf(':');
      if (sep > 0) {
        return { kind: 'field', path: token.v.slice(0, sep), value: token.v.slice(sep + 1) };
      }
    }
    return { kind: 'text', value: token.v };
  }

  const root = parseExpr();
  if (pos !== tokens.length) throw new MockApiError('QUERY_SYNTAX_ERROR', '查询串有多余内容');
  return root;
}

export function asSearchText(value: unknown): string {
  switch (typeof value) {
    case 'string':
      return value;
    case 'number':
    case 'boolean':
    case 'bigint':
    case 'symbol':
      return value.toString();
    default:
      return JSON.stringify(value);
  }
}

export interface RowAccessor {
  /** 返回某路径在该行上的值；不存在返回 undefined。 */
  get: (path: string) => unknown;
  body: () => string;
}

function evaluate(node: Node, row: RowAccessor): boolean {
  switch (node.kind) {
    case 'and':
      return evaluate(node.left, row) && evaluate(node.right, row);
    case 'or':
      return evaluate(node.left, row) || evaluate(node.right, row);
    case 'field': {
      const actual = row.get(node.path);
      if (actual === undefined || actual === null) return false;
      return asSearchText(actual).toLowerCase() === node.value.toLowerCase();
    }
    case 'text':
      return row.body().toLowerCase().includes(node.value.toLowerCase());
  }
}

export type QueryPredicate = (row: RowAccessor) => boolean;

export function compileQuery(query: string | undefined): QueryPredicate {
  if (!query || !query.trim()) return () => true;
  const tokens = tokenize(query);
  if (!tokens.length) return () => true;
  const ast = parse(tokens);
  return (row) => evaluate(ast, row);
}

/** 查询串里引用到的字段路径，用于校验路径是否存在于字段目录。 */
export function referencedPaths(query: string | undefined): string[] {
  if (!query || !query.trim()) return [];
  const paths = new Set<string>();
  const visit = (node: Node) => {
    if (node.kind === 'field') paths.add(node.path);
    else if (node.kind === 'and' || node.kind === 'or') {
      visit(node.left);
      visit(node.right);
    }
  };
  visit(parse(tokenize(query)));
  return [...paths];
}
