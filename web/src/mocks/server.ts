import { setupServer } from 'msw/node';

import { handlers } from './handlers';

/** Node 测试环境用的同一套 handlers —— 浏览器与测试共用一份 mock，避免两处行为分叉。 */
export const server = setupServer(...handlers);
