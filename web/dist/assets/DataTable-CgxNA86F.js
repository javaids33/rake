import{c as l,r as u,j as e,h as j,z as v}from"./index-BcarfE41.js";import{C as S}from"./copy-D9zCs6P9.js";/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const z=l("ChevronDown",[["path",{d:"m6 9 6 6 6-6",key:"qrunsl"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const D=l("ChevronUp",[["path",{d:"m18 15-6-6-6 6",key:"153udz"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const L=l("Download",[["path",{d:"M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4",key:"ih7n3h"}],["polyline",{points:"7 10 12 15 17 10",key:"2ggqvy"}],["line",{x1:"12",x2:"12",y1:"15",y2:"3",key:"1vk2je"}]]);/**
 * @license lucide-react v0.468.0 - ISC
 *
 * This source code is licensed under the ISC license.
 * See the LICENSE file in the root directory of this source tree.
 */const R=l("Table2",[["path",{d:"M9 3H5a2 2 0 0 0-2 2v4m6-6h10a2 2 0 0 1 2 2v4M9 3v18m0 0h10a2 2 0 0 0 2-2V9M9 21H5a2 2 0 0 1-2-2V9m0 0h18",key:"gugj83"}]]);function $({columns:a,rows:i,maxHeight:y="400px",onExportCsv:x,compact:h}){const[c,w]=u.useState(null),[m,b]=u.useState("asc"),f=t=>{c===t?b(n=>n==="asc"?"desc":"asc"):(w(t),b("asc"))},d=c?[...i].sort((t,n)=>{const s=t[c],r=n[c],o=String(s??"").localeCompare(String(r??""),void 0,{numeric:!0});return m==="asc"?o:-o}):i,g=()=>{const t=a.join("	"),n=d.map(s=>a.map(r=>String(s[r]??"")).join("	")).join(`
`);navigator.clipboard.writeText(`${t}
${n}`),v.success("Copied to clipboard")},N=()=>{if(x){x();return}const t=a.join(","),n=d.map(C=>a.map(k=>{const p=String(C[k]??"");return p.includes(",")?`"${p}"`:p}).join(",")).join(`
`),s=new Blob([`${t}
${n}`],{type:"text/csv"}),r=URL.createObjectURL(s),o=document.createElement("a");o.href=r,o.download="query-results.csv",o.click(),URL.revokeObjectURL(r),v.success("Downloaded CSV")};return a.length?e.jsxs("div",{className:"flex flex-col",children:[e.jsxs("div",{className:"flex items-center justify-between px-3 py-2 border-b border-white/[0.03]",children:[e.jsxs("span",{className:"text-2xs font-mono text-zinc-600 readout",children:[i.length," row",i.length!==1?"s":""]}),e.jsxs("div",{className:"flex items-center gap-1",children:[e.jsx("button",{onClick:g,className:"p-1.5 rounded-md text-zinc-600 hover:text-amber-400/70 hover:bg-white/[0.03] transition-colors",title:"Copy",children:e.jsx(S,{className:"w-3.5 h-3.5"})}),e.jsx("button",{onClick:N,className:"p-1.5 rounded-md text-zinc-600 hover:text-amber-400/70 hover:bg-white/[0.03] transition-colors",title:"Export CSV",children:e.jsx(L,{className:"w-3.5 h-3.5"})})]})]}),e.jsx("div",{className:"overflow-auto",style:{maxHeight:y},children:e.jsxs("table",{className:"w-full text-left",children:[e.jsx("thead",{className:"sticky top-0 z-10",children:e.jsx("tr",{className:"bg-navy-900/90 backdrop-blur-sm border-b border-white/[0.04]",children:a.map(t=>e.jsx("th",{onClick:()=>f(t),className:j("px-3 py-2 text-2xs font-mono font-semibold text-amber-400/50 cursor-pointer select-none whitespace-nowrap","hover:text-amber-400/80 transition-colors tracking-wider uppercase",h&&"px-2 py-1.5"),children:e.jsxs("span",{className:"inline-flex items-center gap-1",children:[t,c===t&&(m==="asc"?e.jsx(D,{className:"w-3 h-3"}):e.jsx(z,{className:"w-3 h-3"}))]})},t))})}),e.jsx("tbody",{children:d.map((t,n)=>e.jsx("tr",{className:"border-b border-white/[0.02] hover:bg-white/[0.015] transition-colors",children:a.map(s=>e.jsx("td",{className:j("px-3 py-1.5 text-xs font-mono text-zinc-300 whitespace-nowrap max-w-[300px] truncate",h&&"px-2 py-1",typeof t[s]=="number"&&"text-cyan-300 readout",t[s]===null&&"text-zinc-700 italic"),children:t[s]===null?"NULL":String(t[s])},s))},n))})]})})]}):null}export{$ as D,R as T};
