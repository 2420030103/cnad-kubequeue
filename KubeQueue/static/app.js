const $ = id => document.getElementById(id);
let depthHistory = [];
const descriptions = {
  kubequeue: "KubeQueue considers current backlog and a moving-average forecast of recent arrivals.",
  static: "Static baseline keeps a fixed replica count and does not react to queue demand.",
  hpa: "HPA baseline is approximated with a reactive load proxy; this demo does not measure real CPU or memory.",
  keda: "KEDA baseline reacts to current queue depth; it does not predict future arrivals in this simulation."
};
async function api(path, body={}) {
  const response = await fetch(path, {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || "Request failed");
  return data;
}
function note(text){ $("notice").textContent = text; }
async function submitTask(){
  try { const d = await api("/api/tasks",{name:$("taskName").value || "Demo task",priority:$("priority").value}); note(`Added ${d.task.id} (${d.task.priority}). ${d.scaling.reason}`); await refresh(); }
  catch(e){note(e.message);}
}
async function surge(count){
  try {const d=await api("/api/surge",{count});note(`Load surge generated: ${d.added} tasks. ${d.scaling.reason}`);await refresh();}
  catch(e){note(e.message);}
}
async function processCustom(count){
  try {const d=await api("/api/process",{count});note(`Processed ${d.count} tasks. ${d.scaling.reason}`);await refresh();}
  catch(e){note(e.message);}
}
async function processTasks(){
  try {const d=await api("/api/process",{});note(`Processed ${d.count} tasks using priority dispatch. ${d.scaling.reason}`);await refresh();}
  catch(e){note(e.message);}
}
async function changeStrategy(){
  try {const d=await api("/api/strategy",{strategy:$("strategy").value});$("strategyText").textContent=descriptions[d.strategy];note(`Switched to ${d.strategy.toUpperCase()} comparison strategy.`);await refresh();}
  catch(e){note(e.message);}
}
async function resetDemo(){
  if(!confirm("Reset all demo queues and metrics?")) return;
  await api("/api/reset"); depthHistory=[]; note("Demo reset."); await refresh();
}
function setBar(id,value,max){$(id).style.width=(max?Math.max(0,Math.min(100,value/max*100)):0)+"%";}
function drawChart(){
  const c=$("chart"), ctx=c.getContext("2d"), w=c.width, h=c.height;
  ctx.clearRect(0,0,w,h);
  ctx.strokeStyle="#e9edf3";ctx.lineWidth=1;
  for(let i=0;i<4;i++){const y=10+i*(h-25)/3;ctx.beginPath();ctx.moveTo(30,y);ctx.lineTo(w-8,y);ctx.stroke();}
  if(depthHistory.length<2){ctx.fillStyle="#98a2b3";ctx.font="12px sans-serif";ctx.fillText("Add tasks to generate queue-depth history",40,h/2);return;}
  const max=Math.max(5,...depthHistory), points=depthHistory.map((v,i)=>({x:30+i*(w-45)/(depthHistory.length-1),y:h-15-v/max*(h-35)}));
  ctx.beginPath();points.forEach((p,i)=>i?ctx.lineTo(p.x,p.y):ctx.moveTo(p.x,p.y));ctx.strokeStyle="#c5222a";ctx.lineWidth=3;ctx.stroke();
  ctx.lineTo(points[points.length-1].x,h-15);ctx.lineTo(points[0].x,h-15);ctx.closePath();ctx.fillStyle="rgba(197,34,42,.08)";ctx.fill();
}
function renderEvents(events){
  $("events").innerHTML=events.length?events.map(e=>`<div class="event"><span class="event-dot ${e.type||""}"></span><div><time>${e.time}</time><p>${escapeHtml(e.message)}</p></div></div>`).join(""):'<div class="empty">No events yet. Add tasks to begin.</div>';
}
function escapeHtml(s){return String(s).replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));}
function renderCompleted(items){
  $("completed").innerHTML=items.length?items.map(t=>`<tr><td>${escapeHtml(t.id)}</td><td>${escapeHtml(t.name)}</td><td><span class="badge ${t.priority}">${t.priority}</span></td><td>${t.created_at}</td><td>${t.processed_at}</td><td>${t.latency_seconds}s</td></tr>`).join(""):'<tr><td colspan="6" class="empty">No completed tasks yet.</td></tr>';
}
async function refresh(){
  try{
    const d=await (await fetch("/api/status")).json();
    $("depth").textContent=d.queue_depth;$("processed").textContent=d.processed;$("forecast").textContent=d.forecast_arrivals;
    $("forecast2").textContent=d.forecast_arrivals;$("throughput").textContent=d.throughput_per_min;
    $("replicas").textContent=d.replicas;$("replicas2").textContent=d.replicas;
    $("scaleReason").textContent=d.strategy.toUpperCase()+" scaling decision";
    $("strategy").value=d.strategy;$("strategyText").textContent=descriptions[d.strategy];
    ["critical","normal","batch"].forEach(k=>{$(k).textContent=d[k+"_depth"];setBar(k+"Bar",d[k+"_depth"],Math.max(1,d.queue_depth));});
    depthHistory.push(d.queue_depth);if(depthHistory.length>24)depthHistory.shift();drawChart();
    renderEvents(d.events||[]);renderCompleted(d.recent_completed||[]);
    $("clock").textContent=new Date().toLocaleTimeString();
  }catch(e){note("Could not refresh dashboard. Is the server running?");}
}
refresh();setInterval(refresh,2500);

async function runBenchmark(){
  try{
    const d=await (await fetch('/api/benchmark')).json();
    $('benchmark').innerHTML=d.results.map(r=>`<tr><td><strong>${escapeHtml(r.strategy)}</strong></td><td>${r.replicas}</td><td>${r.estimated_capacity_per_batch} tasks</td><td>${r.estimated_drain_seconds}s</td></tr>`).join('');
    $('benchmarkNote').textContent=d.disclaimer;
  }catch(e){$('benchmarkNote').textContent='Benchmark unavailable: '+e.message;}
}
