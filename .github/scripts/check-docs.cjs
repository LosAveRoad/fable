'use strict';
function check(body,files) {
 const text=body||'';
 const yes=/^-\s*\[[xX]\]\s*需要(?:[（(\s]|$)/m.test(text);
 const no=/^-\s*\[[xX]\]\s*不需要(?:[（(\s]|$)/m.test(text);
 if(Number(yes)+Number(no)!==1)throw new Error('PR 必须恰好选择一项文档影响：需要或不需要。');
 if(yes&&!files.some(p=>/^(docs\/|api\/openapi\/|api\/proto\/)|(^|\/)README(?:\.[^/]*)?$/i.test(p)))throw new Error('选择需要修改文档，但没有文档或接口生成物变更。');
 if(no) {
  const line=text.split('\n').find(x=>/^-\s*\[[xX]\]\s*不需要/.test(x))||'';
  const reason=(line.match(/原因[：:]\s*(.*)/)||[])[1]||'';
  if(reason.replace(/[）)\s.。…]/g,'').length<8)throw new Error('选择不需要修改文档时，必须在同一行说明具体原因。');
 }
}
module.exports={check};
