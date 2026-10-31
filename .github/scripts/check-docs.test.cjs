const {test}=require('node:test');
const assert=require('node:assert/strict');
const {check}=require('./check-docs.cjs');
test('documentation required must include a changed maintained document',()=>{
 assert.doesNotThrow(()=>check('- [x] 需要\n- [ ] 不需要\n', ['docs/contracts/logic.md']));
 assert.throws(()=>check('- [x] 需要\n- [ ] 不需要\n', ['internal/logic/service.go']));
 assert.doesNotThrow(()=>check('- [x] 需要', ['api/openapi/http.json']));
});
test('exactly one decision and a concrete reason when no docs change',()=>{
 for(const body of ['', '- [x] 需要\n- [x] 不需要','- [ ] 需要\n- [ ] 不需要','- [x] 不需要（原因：...）'])assert.throws(()=>check(body,[]));
 assert.doesNotThrow(()=>check('- [x] 不需要（原因：仅修正内部超时清理，接口和角色行为保持不变。）',[]));
});
