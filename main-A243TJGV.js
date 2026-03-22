var _w=Object.defineProperty,xw=Object.defineProperties;var Ew=Object.getOwnPropertyDescriptors;var b_=Object.getOwnPropertySymbols;var Mw=Object.prototype.hasOwnProperty,bw=Object.prototype.propertyIsEnumerable;var S_=(n,e,t)=>e in n?_w(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,Ee=(n,e)=>{for(var t in e||={})Mw.call(e,t)&&S_(n,t,e[t]);if(b_)for(var t of b_(e))bw.call(e,t)&&S_(n,t,e[t]);return n},Ut=(n,e)=>xw(n,Ew(e));var In=null,iu=!1,Mp=1,Sw=null,Qn=Symbol("SIGNAL");function Ke(n){let e=In;return In=n,e}function ou(){return In}var ac={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function cc(n){if(iu)throw new Error("");if(In===null)return;In.consumerOnSignalRead(n);let e=In.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=In.recomputing;if(i&&(t=e!==void 0?e.nextProducer:In.producers,t!==void 0&&t.producer===n)){In.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===In&&(!i||Tw(r,In)))return;let o=As(In),s={producer:n,consumer:In,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};In.producersTail=s,e!==void 0?e.nextProducer=s:In.producers=s,o&&I_(n,s)}function w_(){Mp++}function bp(n){if(!(As(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Mp)){if(!n.producerMustRecompute(n)&&!au(n)){ru(n);return}n.producerRecomputeValue(n),ru(n)}}function Sp(n){if(n.consumers===void 0)return;let e=iu;iu=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||ww(i)}}finally{iu=e}}function wp(){return In?.consumerAllowSignalWrites!==!1}function ww(n){n.dirty=!0,Sp(n),n.consumerMarkedDirty?.(n)}function ru(n){n.dirty=!1,n.lastCleanEpoch=Mp}function lc(n){return n&&T_(n),Ke(n)}function T_(n){n.producersTail=void 0,n.recomputing=!0}function su(n,e){Ke(e),n&&C_(n)}function C_(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(As(n))do t=Tp(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function au(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(bp(t),i!==t.version))return!0}return!1}function uc(n){if(As(n)){let e=n.producers;for(;e!==void 0;)e=Tp(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function I_(n,e){let t=n.consumersTail,i=As(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)I_(r.producer,r)}function Tp(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!As(e)){let o=e.producers;for(;o!==void 0;)o=Tp(o)}return t}function As(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Cp(n){Sw?.(n)}function Tw(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function Ip(n,e){return Object.is(n,e)}function Cw(){throw new Error}var A_=Cw;function R_(n){A_(n)}function Ap(n){A_=n}var Iw=null;function Rp(n,e){let t=Object.create(cu);t.value=n,e!==void 0&&(t.equal=e);let i=()=>D_(t);return i[Qn]=t,Cp(t),[i,s=>Rs(t,s),s=>Dp(t,s)]}function D_(n){return cc(n),n.value}function Rs(n,e){wp()||R_(n),n.equal(n.value,e)||(n.value=e,Aw(n))}function Dp(n,e){wp()||R_(n),Rs(n,e(n.value))}var cu=Ut(Ee({},ac),{equal:Ip,value:void 0,kind:"signal"});function Aw(n){n.version++,w_(),Sp(n),Iw?.(n)}function Ye(n){return typeof n=="function"}function Ds(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var lu=Ds(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function dc(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var En=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let o of t)o.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Ye(i))try{i()}catch(o){e=o instanceof lu?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{N_(o)}catch(s){e=e??[],s instanceof lu?e=[...e,...s.errors]:e.push(s)}}if(e)throw new lu(e)}}add(e){var t;if(e&&e!==this)if(this.closed)N_(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&dc(t,e)}remove(e){let{_finalizers:t}=this;t&&dc(t,e),e instanceof n&&e._removeParent(this)}};En.EMPTY=(()=>{let n=new En;return n.closed=!0,n})();var Np=En.EMPTY;function uu(n){return n instanceof En||n&&"closed"in n&&Ye(n.remove)&&Ye(n.add)&&Ye(n.unsubscribe)}function N_(n){Ye(n)?n():n.unsubscribe()}var Ni={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var Ns={setTimeout(n,e,...t){let{delegate:i}=Ns;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=Ns;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function du(n){Ns.setTimeout(()=>{let{onUnhandledError:e}=Ni;if(e)e(n);else throw n})}function fc(){}var P_=Pp("C",void 0,void 0);function L_(n){return Pp("E",void 0,n)}function O_(n){return Pp("N",n,void 0)}function Pp(n,e,t){return{kind:n,value:e,error:t}}var Fo=null;function Ps(n){if(Ni.useDeprecatedSynchronousErrorHandling){let e=!Fo;if(e&&(Fo={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Fo;if(Fo=null,t)throw i}}else n()}function F_(n){Ni.useDeprecatedSynchronousErrorHandling&&Fo&&(Fo.errorThrown=!0,Fo.error=n)}var ko=class extends En{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,uu(e)&&e.add(this)):this.destination=Nw}static create(e,t,i){return new Ls(e,t,i)}next(e){this.isStopped?Op(O_(e),this):this._next(e)}error(e){this.isStopped?Op(L_(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Op(P_,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Rw=Function.prototype.bind;function Lp(n,e){return Rw.call(n,e)}var Fp=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){fu(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){fu(i)}else fu(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){fu(t)}}},Ls=class extends ko{constructor(e,t,i){super();let r;if(Ye(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let o;this&&Ni.useDeprecatedNextContext?(o=Object.create(e),o.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Lp(e.next,o),error:e.error&&Lp(e.error,o),complete:e.complete&&Lp(e.complete,o)}):r=e}this.destination=new Fp(r)}};function fu(n){Ni.useDeprecatedSynchronousErrorHandling?F_(n):du(n)}function Dw(n){throw n}function Op(n,e){let{onStoppedNotification:t}=Ni;t&&Ns.setTimeout(()=>t(n,e))}var Nw={closed:!0,next:fc,error:Dw,complete:fc};var Os=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Pi(n){return n}function kp(...n){return Up(n)}function Up(n){return n.length===0?Pi:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Mt=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let o=Lw(t)?t:new Ls(t,i,r);return Ps(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=k_(i),new i((r,o)=>{let s=new Ls({next:a=>{try{t(a)}catch(c){o(c),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Os](){return this}pipe(...t){return Up(t)(this)}toPromise(t){return t=k_(t),new t((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return n.create=e=>new n(e),n})();function k_(n){var e;return(e=n??Ni.Promise)!==null&&e!==void 0?e:Promise}function Pw(n){return n&&Ye(n.next)&&Ye(n.error)&&Ye(n.complete)}function Lw(n){return n&&n instanceof ko||Pw(n)&&uu(n)}function Ow(n){return Ye(n?.lift)}function Tt(n){return e=>{if(Ow(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function Nt(n,e,t,i,r){return new Bp(n,e,t,i,r)}var Bp=class extends ko{constructor(e,t,i,r,o,s){super(e),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var U_=Ds(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var Mn=(()=>{class n extends Mt{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new hu(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new U_}next(t){Ps(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Ps(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Ps(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:o}=this;return i||r?Np:(this.currentObservers=null,o.push(t),new En(()=>{this.currentObservers=null,dc(o,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:o}=this;i?t.error(r):o&&t.complete()}asObservable(){let t=new Mt;return t.source=this,t}}return n.create=(e,t)=>new hu(e,t),n})(),hu=class extends Mn{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Np}};var bn=class extends Mn{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};var Sn=new Mt(n=>n.complete());function B_(n){return n&&Ye(n.schedule)}function H_(n){return n[n.length-1]}function V_(n){return Ye(H_(n))?n.pop():void 0}function zr(n){return B_(H_(n))?n.pop():void 0}function G_(n,e,t,i){function r(o){return o instanceof t?o:new t(function(s){s(o)})}return new(t||(t=Promise))(function(o,s){function a(u){try{l(i.next(u))}catch(d){s(d)}}function c(u){try{l(i.throw(u))}catch(d){s(d)}}function l(u){u.done?o(u.value):r(u.value).then(a,c)}l((i=i.apply(n,e||[])).next())})}function z_(n){var e=typeof Symbol=="function"&&Symbol.iterator,t=e&&n[e],i=0;if(t)return t.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(e?"Object is not iterable.":"Symbol.iterator is not defined.")}function Uo(n){return this instanceof Uo?(this.v=n,this):new Uo(n)}function W_(n,e,t){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=t.apply(n,e||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(h){return function(m){return Promise.resolve(m).then(h,d)}}function a(h,m){i[h]&&(r[h]=function(y){return new Promise(function(g,p){o.push([h,y,g,p])>1||c(h,y)})},m&&(r[h]=m(r[h])))}function c(h,m){try{l(i[h](m))}catch(y){f(o[0][3],y)}}function l(h){h.value instanceof Uo?Promise.resolve(h.value.v).then(u,d):f(o[0][2],h)}function u(h){c("next",h)}function d(h){c("throw",h)}function f(h,m){h(m),o.shift(),o.length&&c(o[0][0],o[0][1])}}function j_(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var e=n[Symbol.asyncIterator],t;return e?e.call(n):(n=typeof z_=="function"?z_(n):n[Symbol.iterator](),t={},i("next"),i("throw"),i("return"),t[Symbol.asyncIterator]=function(){return this},t);function i(o){t[o]=n[o]&&function(s){return new Promise(function(a,c){s=n[o](s),r(a,c,s.done,s.value)})}}function r(o,s,a,c){Promise.resolve(c).then(function(l){o({value:l,done:a})},s)}}var pu=n=>n&&typeof n.length=="number"&&typeof n!="function";function mu(n){return Ye(n?.then)}function gu(n){return Ye(n[Os])}function vu(n){return Symbol.asyncIterator&&Ye(n?.[Symbol.asyncIterator])}function yu(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Fw(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var _u=Fw();function xu(n){return Ye(n?.[_u])}function Eu(n){return W_(this,arguments,function*(){let t=n.getReader();try{for(;;){let{value:i,done:r}=yield Uo(t.read());if(r)return yield Uo(void 0);yield yield Uo(i)}}finally{t.releaseLock()}})}function Mu(n){return Ye(n?.getReader)}function hn(n){if(n instanceof Mt)return n;if(n!=null){if(gu(n))return kw(n);if(pu(n))return Uw(n);if(mu(n))return Bw(n);if(vu(n))return $_(n);if(xu(n))return Hw(n);if(Mu(n))return Vw(n)}throw yu(n)}function kw(n){return new Mt(e=>{let t=n[Os]();if(Ye(t.subscribe))return t.subscribe(e);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function Uw(n){return new Mt(e=>{for(let t=0;t<n.length&&!e.closed;t++)e.next(n[t]);e.complete()})}function Bw(n){return new Mt(e=>{n.then(t=>{e.closed||(e.next(t),e.complete())},t=>e.error(t)).then(null,du)})}function Hw(n){return new Mt(e=>{for(let t of n)if(e.next(t),e.closed)return;e.complete()})}function $_(n){return new Mt(e=>{zw(n,e).catch(t=>e.error(t))})}function Vw(n){return $_(Eu(n))}function zw(n,e){var t,i,r,o;return G_(this,void 0,void 0,function*(){try{for(t=j_(n);i=yield t.next(),!i.done;){let s=i.value;if(e.next(s),e.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=t.return)&&(yield o.call(t))}finally{if(r)throw r.error}}e.complete()})}function jn(n,e,t,i=0,r=!1){let o=e.schedule(function(){t(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(o),!r)return o}function bu(n,e=0){return Tt((t,i)=>{t.subscribe(Nt(i,r=>jn(i,n,()=>i.next(r),e),()=>jn(i,n,()=>i.complete(),e),r=>jn(i,n,()=>i.error(r),e)))})}function Su(n,e=0){return Tt((t,i)=>{i.add(n.schedule(()=>t.subscribe(i),e))})}function q_(n,e){return hn(n).pipe(Su(e),bu(e))}function X_(n,e){return hn(n).pipe(Su(e),bu(e))}function Y_(n,e){return new Mt(t=>{let i=0;return e.schedule(function(){i===n.length?t.complete():(t.next(n[i++]),t.closed||this.schedule())})})}function Z_(n,e){return new Mt(t=>{let i;return jn(t,e,()=>{i=n[_u](),jn(t,e,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){t.error(s);return}o?t.complete():t.next(r)},0,!0)}),()=>Ye(i?.return)&&i.return()})}function wu(n,e){if(!n)throw new Error("Iterable cannot be null");return new Mt(t=>{jn(t,e,()=>{let i=n[Symbol.asyncIterator]();jn(t,e,()=>{i.next().then(r=>{r.done?t.complete():t.next(r.value)})},0,!0)})})}function K_(n,e){return wu(Eu(n),e)}function J_(n,e){if(n!=null){if(gu(n))return q_(n,e);if(pu(n))return Y_(n,e);if(mu(n))return X_(n,e);if(vu(n))return wu(n,e);if(xu(n))return Z_(n,e);if(Mu(n))return K_(n,e)}throw yu(n)}function tn(n,e){return e?J_(n,e):hn(n)}function dt(...n){let e=zr(n);return tn(n,e)}function Hp(n,e){let t=Ye(n)?n:()=>n,i=r=>r.error(t());return new Mt(e?r=>e.schedule(i,0,r):i)}function Tu(n){return!!n&&(n instanceof Mt||Ye(n.lift)&&Ye(n.subscribe))}var Bo=Ds(n=>function(){n(this),this.name="EmptyError",this.message="no elements in sequence"});function qt(n,e){return Tt((t,i)=>{let r=0;t.subscribe(Nt(i,o=>{i.next(n.call(e,o,r++))}))})}var{isArray:Gw}=Array;function Ww(n,e){return Gw(e)?n(...e):n(e)}function Q_(n){return qt(e=>Ww(n,e))}var{isArray:jw}=Array,{getPrototypeOf:$w,prototype:qw,keys:Xw}=Object;function e0(n){if(n.length===1){let e=n[0];if(jw(e))return{args:e,keys:null};if(Yw(e)){let t=Xw(e);return{args:t.map(i=>e[i]),keys:t}}}return{args:n,keys:null}}function Yw(n){return n&&typeof n=="object"&&$w(n)===qw}function t0(n,e){return n.reduce((t,i,r)=>(t[i]=e[r],t),{})}function Vp(...n){let e=zr(n),t=V_(n),{args:i,keys:r}=e0(n);if(i.length===0)return tn([],e);let o=new Mt(Zw(i,e,r?s=>t0(r,s):Pi));return t?o.pipe(Q_(t)):o}function Zw(n,e,t=Pi){return i=>{n0(e,()=>{let{length:r}=n,o=new Array(r),s=r,a=r;for(let c=0;c<r;c++)n0(e,()=>{let l=tn(n[c],e),u=!1;l.subscribe(Nt(i,d=>{o[c]=d,u||(u=!0,a--),a||i.next(t(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function n0(n,e,t){n?jn(t,n,e):e()}function i0(n,e,t,i,r,o,s,a){let c=[],l=0,u=0,d=!1,f=()=>{d&&!c.length&&!l&&e.complete()},h=y=>l<i?m(y):c.push(y),m=y=>{o&&e.next(y),l++;let g=!1;hn(t(y,u++)).subscribe(Nt(e,p=>{r?.(p),o?h(p):e.next(p)},()=>{g=!0},void 0,()=>{if(g)try{for(l--;c.length&&l<i;){let p=c.shift();s?jn(e,s,()=>m(p)):m(p)}f()}catch(p){e.error(p)}}))};return n.subscribe(Nt(e,h,()=>{d=!0,f()})),()=>{a?.()}}function Un(n,e,t=1/0){return Ye(e)?Un((i,r)=>qt((o,s)=>e(i,o,r,s))(hn(n(i,r))),t):(typeof e=="number"&&(t=e),Tt((i,r)=>i0(i,r,n,t)))}function r0(n=1/0){return Un(Pi,n)}function o0(){return r0(1)}function Fs(...n){return o0()(tn(n,zr(n)))}function hc(n){return new Mt(e=>{hn(n()).subscribe(e)})}function xr(n,e){return Tt((t,i)=>{let r=0;t.subscribe(Nt(i,o=>n.call(e,o,r++)&&i.next(o)))})}function pc(n){return Tt((e,t)=>{let i=null,r=!1,o;i=e.subscribe(Nt(t,void 0,void 0,s=>{o=hn(n(s,pc(n)(e))),i?(i.unsubscribe(),i=null,o.subscribe(t)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(t))})}function Cu(n,e){return Ye(e)?Un(n,e,1):Un(n,1)}function s0(n){return Tt((e,t)=>{let i=!1;e.subscribe(Nt(t,r=>{i=!0,t.next(r)},()=>{i||t.next(n),t.complete()}))})}function Er(n){return n<=0?()=>Sn:Tt((e,t)=>{let i=0;e.subscribe(Nt(t,r=>{++i<=n&&(t.next(r),n<=i&&t.complete())}))})}function a0(n=Kw){return Tt((e,t)=>{let i=!1;e.subscribe(Nt(t,r=>{i=!0,t.next(r)},()=>i?t.complete():t.error(n())))})}function Kw(){return new Bo}function zp(n){return Tt((e,t)=>{try{e.subscribe(t)}finally{t.add(n)}})}function Mr(n,e){let t=arguments.length>=2;return i=>i.pipe(n?xr((r,o)=>n(r,o,i)):Pi,Er(1),t?s0(e):a0(()=>new Bo))}function Iu(n){return n<=0?()=>Sn:Tt((e,t)=>{let i=[];e.subscribe(Nt(t,r=>{i.push(r),n<i.length&&i.shift()},()=>{for(let r of i)t.next(r);t.complete()},void 0,()=>{i=null}))})}function Gp(...n){let e=zr(n);return Tt((t,i)=>{(e?Fs(n,t,e):Fs(n,t)).subscribe(i)})}function Li(n,e){return Tt((t,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();t.subscribe(Nt(i,c=>{r?.unsubscribe();let l=0,u=o++;hn(n(c,u)).subscribe(r=Nt(i,d=>i.next(e?e(c,d,u,l++):d),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function mc(n){return Tt((e,t)=>{hn(n).subscribe(Nt(t,()=>t.complete(),fc)),!t.closed&&e.subscribe(t)})}function gi(n,e,t){let i=Ye(n)||e||t?{next:n,error:e,complete:t}:n;return i?Tt((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(Nt(o,c=>{var l;(l=i.next)===null||l===void 0||l.call(i,c),o.next(c)},()=>{var c;a=!1,(c=i.complete)===null||c===void 0||c.call(i),o.complete()},c=>{var l;a=!1,(l=i.error)===null||l===void 0||l.call(i,c),o.error(c)},()=>{var c,l;a&&((c=i.unsubscribe)===null||c===void 0||c.call(i)),(l=i.finalize)===null||l===void 0||l.call(i)}))}):Pi}var Wp;function Au(){return Wp}function Ki(n){let e=Wp;return Wp=n,e}var c0=Symbol("NotFound");function ks(n){return n===c0||n?.name==="\u0275NotFound"}function l0(n){let e=Ke(null);try{return n()}finally{Ke(e)}}var om="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Oe=class extends Error{code;constructor(e,t){super(Ec(e,t)),this.code=e}};function tT(n){return`NG0${Math.abs(n)}`}function Ec(n,e){return`${tT(n)}${e?": "+e:""}`}function Ct(n){for(let e in n)if(n[e]===Ct)return e;throw Error("")}function Mc(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Mc).join(", ")}]`;if(n==null)return""+n;let e=n.overriddenName||n.name;if(e)return`${e}`;let t=n.toString();if(t==null)return""+t;let i=t.indexOf(`
`);return i>=0?t.slice(0,i):t}function sm(n,e){return n?e?`${n} ${e}`:n:e||""}var nT=Ct({__forward_ref__:Ct});function Ou(n){return n.__forward_ref__=Ou,n}function $n(n){return am(n)?n():n}function am(n){return typeof n=="function"&&n.hasOwnProperty(nT)&&n.__forward_ref__===Ou}function je(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function bc(n){return iT(n,Fu)}function cm(n){return bc(n)!==null}function iT(n,e){return n.hasOwnProperty(e)&&n[e]||null}function rT(n){let e=n?.[Fu]??null;return e||null}function $p(n){return n&&n.hasOwnProperty(Du)?n[Du]:null}var Fu=Ct({\u0275prov:Ct}),Du=Ct({\u0275inj:Ct}),Ve=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=je({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function lm(n){return n&&!!n.\u0275providers}var um=Ct({\u0275cmp:Ct}),dm=Ct({\u0275dir:Ct}),fm=Ct({\u0275pipe:Ct}),hm=Ct({\u0275mod:Ct}),vc=Ct({\u0275fac:Ct}),Wo=Ct({__NG_ELEMENT_ID__:Ct}),u0=Ct({__NG_ENV_ID__:Ct});function pm(n){return ku(n,"@NgModule"),n[hm]||null}function jr(n){return ku(n,"@Component"),n[um]||null}function mm(n){return ku(n,"@Directive"),n[dm]||null}function p0(n){return ku(n,"@Pipe"),n[fm]||null}function ku(n,e){if(n==null)throw new Oe(-919,!1)}function m0(n){return typeof n=="string"?n:n==null?"":String(n)}var g0=Ct({ngErrorCode:Ct}),oT=Ct({ngErrorMessage:Ct}),sT=Ct({ngTokenPath:Ct});function gm(n,e){return v0("",-200,e)}function Uu(n,e){throw new Oe(-201,!1)}function v0(n,e,t){let i=new Oe(e,n);return i[g0]=e,i[oT]=n,t&&(i[sT]=t),i}function aT(n){return n[g0]}var qp;function y0(){return qp}function ei(n){let e=qp;return qp=n,e}function vm(n,e,t){let i=bc(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Uu(n,"")}var cT={},Ho=cT,lT="__NG_DI_FLAG__",Xp=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=Vo(t)||0;try{return this.injector.get(e,i&8?null:Ho,i)}catch(r){if(ks(r))return r;throw r}}};function uT(n,e=0){let t=Au();if(t===void 0)throw new Oe(-203,!1);if(t===null)return vm(n,void 0,e);{let i=dT(e),r=t.retrieve(n,i);if(ks(r)){if(i.optional)return null;throw r}return r}}function it(n,e=0){return(y0()||uT)($n(n),e)}function ce(n,e){return it(n,Vo(e))}function Vo(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function dT(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function Yp(n){let e=[];for(let t=0;t<n.length;t++){let i=$n(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Oe(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],c=fT(a);typeof c=="number"?c===-1?r=a.token:o|=c:r=a}e.push(it(r,o))}else e.push(it(i))}return e}function fT(n){return n[lT]}function zo(n,e){let t=n.hasOwnProperty(vc);return t?n[vc]:null}function _0(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],o=e[i];if(t&&(r=t(r),o=t(o)),o!==r)return!1}return!0}function x0(n){return n.flat(Number.POSITIVE_INFINITY)}function Bu(n,e){n.forEach(t=>Array.isArray(t)?Bu(t,e):e(t))}function ym(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function Sc(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function E0(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let o=r-2;n[r]=n[o],r--}n[e]=t,n[e+1]=i}}function M0(n,e,t){let i=Hs(n,e);return i>=0?n[i|1]=t:(i=~i,E0(n,i,e,t)),i}function Hu(n,e){let t=Hs(n,e);if(t>=0)return n[t|1]}function Hs(n,e){return hT(n,e,1)}function hT(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let o=i+(r-i>>1),s=n[o<<t];if(e===s)return o<<t;s>e?r=o:i=o+1}return~(r<<t)}var jo={},Gr=[],$o=new Ve(""),_m=new Ve("",-1),xm=new Ve(""),yc=class{get(e,t=Ho){if(t===Ho){let r=v0("",-201);throw r.name="\u0275NotFound",r}return t}};function Vs(n){return{\u0275providers:n}}function b0(n){return Vs([{provide:$o,multi:!0,useValue:n}])}function S0(...n){return{\u0275providers:Em(!0,n),\u0275fromNgModule:!0}}function Em(n,...e){let t=[],i=new Set,r,o=s=>{t.push(s)};return Bu(e,s=>{let a=s;Nu(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&w0(r,o),t}function w0(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];Mm(r,o=>{e(o,i)})}}function Nu(n,e,t,i){if(n=$n(n),!n)return!1;let r=null,o=$p(n),s=!o&&jr(n);if(!o&&!s){let c=n.ngModule;if(o=$p(c),o)r=c;else return!1}else{if(s&&!s.standalone)return!1;r=n}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let c=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let l of c)Nu(l,e,t,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let l;Bu(o.imports,u=>{Nu(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&w0(l,e)}if(!a){let l=zo(r)||(()=>new r);e({provide:r,useFactory:l,deps:Gr},r),e({provide:xm,useValue:r,multi:!0},r),e({provide:$o,useValue:()=>it(r),multi:!0},r)}let c=o.providers;if(c!=null&&!a){let l=n;Mm(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function Mm(n,e){for(let t of n)lm(t)&&(t=t.\u0275providers),Array.isArray(t)?Mm(t,e):e(t)}var pT=Ct({provide:String,useValue:Ct});function T0(n){return n!==null&&typeof n=="object"&&pT in n}function mT(n){return!!(n&&n.useExisting)}function gT(n){return!!(n&&n.useFactory)}function Pu(n){return typeof n=="function"}var wc=new Ve(""),Ru={},d0={},jp;function Tc(){return jp===void 0&&(jp=new yc),jp}var rn=class{},Go=class extends rn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Kp(e,s=>this.processProvider(s)),this.records.set(_m,Us(void 0,this)),r.has("environment")&&this.records.set(rn,Us(void 0,this));let o=this.records.get(wc);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(xm,Gr,{self:!0}))}retrieve(e,t){let i=Vo(t)||0;try{return this.get(e,Ho,i)}catch(r){if(ks(r))return r;throw r}}destroy(){gc(this),this._destroyed=!0;let e=Ke(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),Ke(e)}}onDestroy(e){return gc(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){gc(this);let t=Ki(this),i=ei(void 0),r;try{return e()}finally{Ki(t),ei(i)}}get(e,t=Ho,i){if(gc(this),e.hasOwnProperty(u0))return e[u0](this);let r=Vo(i),o,s=Ki(this),a=ei(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=ET(e)&&bc(e);u&&this.injectableDefInScope(u)?l=Us(Zp(e),Ru):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Tc():this.parent;return t=r&8&&t===Ho?null:t,c.get(e,t)}catch(c){let l=aT(c);throw l===-200||l===-201?new Oe(l,null):c}finally{ei(a),Ki(s)}}resolveInjectorInitializers(){let e=Ke(null),t=Ki(this),i=ei(void 0),r;try{let o=this.get($o,Gr,{self:!0});for(let s of o)s()}finally{Ki(t),ei(i),Ke(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=$n(e);let t=Pu(e)?e:$n(e&&e.provide),i=yT(e);if(!Pu(e)&&e.multi===!0){let r=this.records.get(t);r||(r=Us(void 0,Ru,!0),r.factory=()=>Yp(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=Ke(null);try{if(t.value===d0)throw gm("");return t.value===Ru&&(t.value=d0,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&xT(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{Ke(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=$n(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function Zp(n){let e=bc(n),t=e!==null?e.factory:zo(n);if(t!==null)return t;if(n instanceof Ve)throw new Oe(-204,!1);if(n instanceof Function)return vT(n);throw new Oe(-204,!1)}function vT(n){if(n.length>0)throw new Oe(-204,!1);let t=rT(n);return t!==null?()=>t.factory(n):()=>new n}function yT(n){if(T0(n))return Us(void 0,n.useValue);{let e=C0(n);return Us(e,Ru)}}function C0(n,e,t){let i;if(Pu(n)){let r=$n(n);return zo(r)||Zp(r)}else if(T0(n))i=()=>$n(n.useValue);else if(gT(n))i=()=>n.useFactory(...Yp(n.deps||[]));else if(mT(n))i=(r,o)=>it($n(n.useExisting),o!==void 0&&o&8?8:void 0);else{let r=$n(n&&(n.useClass||n.provide));if(_T(n))i=()=>new r(...Yp(n.deps));else return zo(r)||Zp(r)}return i}function gc(n){if(n.destroyed)throw new Oe(-205,!1)}function Us(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function _T(n){return!!n.deps}function xT(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function ET(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Kp(n,e){for(let t of n)Array.isArray(t)?Kp(t,e):t&&lm(t)?Kp(t.\u0275providers,e):e(t)}function An(n,e){let t;n instanceof Go?(gc(n),t=n):t=new Xp(n);let i,r=Ki(t),o=ei(void 0);try{return e()}finally{Ki(r),ei(o)}}function I0(){return y0()!==void 0||Au()!=null}var Oi=0,$e=1,Je=2,pn=3,vi=4,yi=5,Cc=6,zs=7,wn=8,$r=9,Qi=10,mn=11,Gs=12,bm=13,qo=14,_i=15,qr=16,Xo=17,er=18,Xr=19,Sm=20,br=21,Vu=22,Ic=23,ni=24,zu=25,Ws=26,Rn=27,A0=1;var Yr=7,Ac=8,Yo=9,Dn=10;function Zr(n){return Array.isArray(n)&&typeof n[A0]=="object"}function Fi(n){return Array.isArray(n)&&n[A0]===!0}function wm(n){return(n.flags&4)!==0}function Kr(n){return n.componentOffset>-1}function Gu(n){return(n.flags&1)===1}function Zo(n){return!!n.template}function js(n){return(n[Je]&512)!==0}function Ko(n){return(n[Je]&256)===256}var Tm="svg",R0="math";function xi(n){for(;Array.isArray(n);)n=n[Oi];return n}function Cm(n,e){return xi(e[n])}function tr(n,e){return xi(e[n.index])}function Wu(n,e){return n.data[e]}function nr(n,e){let t=e[n];return Zr(t)?t:t[Oi]}function D0(n){return(n[Je]&4)===4}function ju(n){return(n[Je]&128)===128}function N0(n){return Fi(n[pn])}function Jr(n,e){return e==null?null:n[e]}function Im(n){n[Xo]=0}function Am(n){n[Je]&1024||(n[Je]|=1024,ju(n)&&Dc(n))}function P0(n,e){for(;n>0;)e=e[qo],n--;return e}function Rc(n){return!!(n[Je]&9216||n[ni]?.dirty)}function $u(n){n[Qi].changeDetectionScheduler?.notify(8),n[Je]&64&&(n[Je]|=1024),Rc(n)&&Dc(n)}function Dc(n){n[Qi].changeDetectionScheduler?.notify(0);let e=Wr(n);for(;e!==null&&!(e[Je]&8192||(e[Je]|=8192,!ju(e)));)e=Wr(e)}function Rm(n,e){if(Ko(n))throw new Oe(911,!1);n[br]===null&&(n[br]=[]),n[br].push(e)}function L0(n,e){if(n[br]===null)return;let t=n[br].indexOf(e);t!==-1&&n[br].splice(t,1)}function Wr(n){let e=n[pn];return Fi(e)?e[pn]:e}function Dm(n){return n[zs]??=[]}function Nm(n){return n.cleanup??=[]}function O0(n,e,t,i){let r=Dm(e);r.push(t),n.firstCreatePass&&Nm(n).push(i,r.length-1)}var rt={lFrame:Y0(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Jp=!1;function F0(){return rt.lFrame.elementDepthCount}function k0(){rt.lFrame.elementDepthCount++}function Pm(){rt.lFrame.elementDepthCount--}function U0(){return rt.bindingsEnabled}function B0(){return rt.skipHydrationRootTNode!==null}function Lm(n){return rt.skipHydrationRootTNode===n}function Om(){rt.skipHydrationRootTNode=null}function Vt(){return rt.lFrame.lView}function ki(){return rt.lFrame.tView}function $s(n){return rt.lFrame.contextLView=n,n[wn]}function qs(n){return rt.lFrame.contextLView=null,n}function Ui(){let n=Fm();for(;n!==null&&n.type===64;)n=n.parent;return n}function Fm(){return rt.lFrame.currentTNode}function H0(){let n=rt.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function Xs(n,e){let t=rt.lFrame;t.currentTNode=n,t.isParent=e}function km(){return rt.lFrame.isParent}function V0(){rt.lFrame.isParent=!1}function Um(){return Jp}function Bm(n){let e=Jp;return Jp=n,e}function z0(n){return rt.lFrame.bindingIndex=n}function qu(){return rt.lFrame.bindingIndex++}function G0(n){let e=rt.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function W0(){return rt.lFrame.inI18n}function j0(n,e){let t=rt.lFrame;t.bindingIndex=t.bindingRootIndex=n,Xu(e)}function $0(){return rt.lFrame.currentDirectiveIndex}function Xu(n){rt.lFrame.currentDirectiveIndex=n}function q0(n){let e=rt.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Hm(){return rt.lFrame.currentQueryIndex}function Yu(n){rt.lFrame.currentQueryIndex=n}function MT(n){let e=n[$e];return e.type===2?e.declTNode:e.type===1?n[yi]:null}function Vm(n,e,t){if(t&4){let r=e,o=n;for(;r=r.parent,r===null&&!(t&1);)if(r=MT(o),r===null||(o=o[qo],r.type&10))break;if(r===null)return!1;e=r,n=o}let i=rt.lFrame=X0();return i.currentTNode=e,i.lView=n,!0}function Zu(n){let e=X0(),t=n[$e];rt.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function X0(){let n=rt.lFrame,e=n===null?null:n.child;return e===null?Y0(n):e}function Y0(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function Z0(){let n=rt.lFrame;return rt.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var zm=Z0;function Ku(){let n=Z0();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function K0(n){return(rt.lFrame.contextLView=P0(n,rt.lFrame.contextLView))[wn]}function Jo(){return rt.lFrame.selectedIndex}function Qr(n){rt.lFrame.selectedIndex=n}function J0(){let n=rt.lFrame;return Wu(n.tView,n.selectedIndex)}function Ei(){rt.lFrame.currentNamespace=Tm}function Mi(){bT()}function bT(){rt.lFrame.currentNamespace=null}function Q0(){return rt.lFrame.currentNamespace}var ex=!0;function Ju(){return ex}function Qu(n){ex=n}function Qp(n,e=null,t=null,i){let r=Gm(n,e,t,i);return r.resolveInjectorInitializers(),r}function Gm(n,e=null,t=null,i,r=new Set){let o=[t||Gr,S0(n)],s;return new Go(o,e||Tc(),s||null,r)}var Ji=class n{static THROW_IF_NOT_FOUND=Ho;static NULL=new yc;static create(e,t){if(Array.isArray(e))return Qp({name:""},t,e,"");{let i=e.name??"";return Qp({name:i},e.parent,e.providers,i)}}static \u0275prov=je({token:n,providedIn:"any",factory:()=>it(_m)});static __NG_ELEMENT_ID__=-1},on=new Ve(""),eo=(()=>{class n{static __NG_ELEMENT_ID__=ST;static __NG_ENV_ID__=t=>t}return n})(),em=class extends eo{_lView;constructor(e){super(),this._lView=e}get destroyed(){return Ko(this._lView)}onDestroy(e){let t=this._lView;return Rm(t,e),()=>L0(t,e)}};function ST(){return new em(Vt())}var tx=!1,nx=new Ve(""),to=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new bn(!1);debugTaskTracker=ce(nx,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Mt(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=je({token:n,providedIn:"root",factory:()=>new n})}return n})(),tm=class extends Mn{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,I0()&&(this.destroyRef=ce(eo,{optional:!0})??void 0,this.pendingTasks=ce(to,{optional:!0})??void 0)}emit(e){let t=Ke(null);try{super.next(e)}finally{Ke(t)}}subscribe(e,t,i){let r=e,o=t||(()=>null),s=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),o=c.error?.bind(c),s=c.complete?.bind(c)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return e instanceof En&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},Bn=tm;function Lu(...n){}function Wm(n){let e,t;function i(){n=Lu;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function ix(n){return queueMicrotask(()=>n()),()=>{n=Lu}}var jm="isAngularZone",_c=jm+"_ID",wT=0,ti=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new Bn(!1);onMicrotaskEmpty=new Bn(!1);onStable=new Bn(!1);onError=new Bn(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=tx}=e;if(typeof Zone>"u")throw new Oe(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,IT(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(jm)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Oe(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Oe(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,e,TT,Lu,Lu);try{return o.runTask(s,t,i)}finally{o.cancelTask(s)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},TT={};function $m(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function CT(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Wm(()=>{n.callbackScheduled=!1,nm(n),n.isCheckStableRunning=!0,$m(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),nm(n)}function IT(n){let e=()=>{CT(n)},t=wT++;n._inner=n._inner.fork({name:"angular",properties:{[jm]:!0,[_c]:t,[_c+t]:!0},onInvokeTask:(i,r,o,s,a,c)=>{if(AT(c))return i.invokeTask(o,s,a,c);try{return f0(n),i.invokeTask(o,s,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),h0(n)}},onInvoke:(i,r,o,s,a,c,l)=>{try{return f0(n),i.invoke(o,s,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!RT(c)&&e(),h0(n)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(n._hasPendingMicrotasks=s.microTask,nm(n),$m(n)):s.change=="macroTask"&&(n.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),n.runOutsideAngular(()=>n.onError.emit(s)),!1)})}function nm(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function f0(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function h0(n){n._nesting--,$m(n)}var xc=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new Bn;onMicrotaskEmpty=new Bn;onStable=new Bn;onError=new Bn;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function AT(n){return rx(n,"__ignore_ng_zone__")}function RT(n){return rx(n,"__scheduler_tick__")}function rx(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var Sr=class{_console=console;handleError(e){this._console.error("ERROR",e)}},ir=new Ve("",{factory:()=>{let n=ce(ti),e=ce(rn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(Sr),t.handleError(i))})}}}),ox={provide:$o,useValue:()=>{let n=ce(Sr,{optional:!0})},multi:!0},DT=new Ve("",{factory:()=>{let n=ce(on).defaultView;if(!n)return;let e=ce(ir),t=o=>{e(o.reason),o.preventDefault()},i=o=>{o.error?e(o.error):e(new Error(o.message,{cause:o})),o.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),ce(eo).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function qm(){return Vs([b0(()=>{ce(DT)})])}function Nn(n,e){let[t,i,r]=Rp(n,e?.equal),o=t,s=o[Qn];return o.set=i,o.update=r,o.asReadonly=sx.bind(o),o}function sx(){let n=this[Qn];if(n.readonlyFn===void 0){let e=()=>this();e[Qn]=n,n.readonlyFn=e}return n.readonlyFn}var Bs=class{},Nc=new Ve("",{factory:()=>!0});var Xm=new Ve("");var Ym=(()=>{class n{static \u0275prov=je({token:n,providedIn:"root",factory:()=>new im})}return n})(),im=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},rm=class{[Qn];constructor(e){this[Qn]=e}destroy(){this[Qn].destroy()}};function xd(n){return{toString:n}.toString()}function HT(n){return typeof n=="function"}function Ox(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var od=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}},Tg=(()=>{let n=()=>Fx;return n.ngInherit=!0,n})();function Fx(n){return n.type.prototype.ngOnChanges&&(n.setInput=zT),VT}function VT(){let n=Ux(this),e=n?.current;if(e){let t=n.previous;if(t===jo)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function zT(n,e,t,i,r){let o=this.declaredInputs[i],s=Ux(n)||GT(n,{previous:jo,current:null}),a=s.current||(s.current={}),c=s.previous,l=c[o];a[o]=new od(l&&l.currentValue,t,c===jo),Ox(n,e,r,t)}var kx="__ngSimpleChanges__";function Ux(n){return n[kx]||null}function GT(n,e){return n[kx]=e}var ax=[];var Pt=function(n,e=null,t){for(let i=0;i<ax.length;i++){let r=ax[i];r(n,e,t)}},bt=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(bt||{});function WT(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=e.type.prototype;if(i){let s=Fx(e);(t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s)}r&&(t.preOrderHooks??=[]).push(0-n,r),o&&((t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o))}function jT(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let o=n.data[t].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=o;s&&(n.contentHooks??=[]).push(-t,s),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function td(n,e,t){Bx(n,e,3,t)}function nd(n,e,t,i){(n[Je]&3)===t&&Bx(n,e,t,i)}function Zm(n,e){let t=n[Je];(t&3)===e&&(t&=16383,t+=1,n[Je]=t)}function Bx(n,e,t,i){let r=i!==void 0?n[Xo]&65535:0,o=i??-1,s=e.length-1,a=0;for(let c=r;c<s;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[Xo]+=65536),(a<o||o==-1)&&($T(n,t,e,c),n[Xo]=(n[Xo]&4294901760)+c+2),c++}function cx(n,e){Pt(bt.LifecycleHookStart,n,e);let t=Ke(null);try{e.call(n)}finally{Ke(t),Pt(bt.LifecycleHookEnd,n,e)}}function $T(n,e,t,i){let r=t[i]<0,o=t[i+1],s=r?-t[i]:t[i],a=n[s];r?n[Je]>>14<n[Xo]>>16&&(n[Je]&3)===e&&(n[Je]+=16384,cx(a,o)):cx(a,o)}var Zs=-1,Oc=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function qT(n){return(n.flags&8)!==0}function XT(n){return(n.flags&16)!==0}function YT(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let o=t[i++],s=t[i++],a=t[i++];n.setAttribute(e,s,a,o)}else{let o=r,s=t[++i];KT(o)?n.setProperty(e,o,s):n.setAttribute(e,o,s),i++}}return i}function ZT(n){return n===3||n===4||n===6}function KT(n){return n.charCodeAt(0)===64}function Ed(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?lx(n,t,r,null,e[++i]):lx(n,t,r,null,null))}}return n}function lx(n,e,t,i,r){let o=0,s=n.length;if(e===-1)s=-1;else for(;o<n.length;){let a=n[o++];if(typeof a=="number"){if(a===e){s=-1;break}else if(a>e){s=o-1;break}}}for(;o<n.length;){let a=n[o];if(typeof a=="number")break;if(a===t){r!==null&&(n[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(n.splice(s,0,e),o=s+1),n.splice(o++,0,t),r!==null&&n.splice(o++,0,r)}function Hx(n){return n!==Zs}function sd(n){return n&32767}function JT(n){return n>>16}function ad(n,e){let t=JT(n),i=e;for(;t>0;)i=i[qo],t--;return i}var ng=!0;function ux(n){let e=ng;return ng=n,e}var QT=256,Vx=QT-1,zx=5,eC=0,rr={};function tC(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(Wo)&&(i=t[Wo]),i==null&&(i=t[Wo]=eC++);let r=i&Vx,o=1<<r;e.data[n+(r>>zx)]|=o}function Gx(n,e){let t=Wx(n,e);if(t!==-1)return t;let i=e[$e];i.firstCreatePass&&(n.injectorIndex=e.length,Km(i.data,n),Km(e,null),Km(i.blueprint,null));let r=Cg(n,e),o=n.injectorIndex;if(Hx(r)){let s=sd(r),a=ad(r,e),c=a[$e].data;for(let l=0;l<8;l++)e[o+l]=a[s+l]|c[s+l]}return e[o+8]=r,o}function Km(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Wx(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Cg(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Yx(r),i===null)return Zs;if(t++,r=r[qo],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return Zs}function nC(n,e,t){tC(n,e,t)}function jx(n,e,t){if(t&8||n!==void 0)return n;Uu(e,"NodeInjector")}function $x(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[$r],o=ei(void 0);try{return r?r.get(e,i,t&8):vm(e,i,t&8)}finally{ei(o)}}return jx(i,e,t)}function qx(n,e,t,i=0,r){if(n!==null){if(e[Je]&2048&&!(i&2)){let s=sC(n,e,t,i,rr);if(s!==rr)return s}let o=Xx(n,e,t,i,rr);if(o!==rr)return o}return $x(e,t,i,r)}function Xx(n,e,t,i,r){let o=rC(t);if(typeof o=="function"){if(!Vm(e,n,i))return i&1?jx(r,t,i):$x(e,t,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Uu(t);else return s}finally{zm()}}else if(typeof o=="number"){let s=null,a=Wx(n,e),c=Zs,l=i&1?e[_i][yi]:null;for((a===-1||i&4)&&(c=a===-1?Cg(n,e):e[a+8],c===Zs||!fx(i,!1)?a=-1:(s=e[$e],a=sd(c),e=ad(c,e)));a!==-1;){let u=e[$e];if(dx(o,a,u.data)){let d=iC(a,e,t,s,i,l);if(d!==rr)return d}c=e[a+8],c!==Zs&&fx(i,e[$e].data[a+8]===l)&&dx(o,a,e)?(s=u,a=sd(c),e=ad(c,e)):a=-1}}return r}function iC(n,e,t,i,r,o){let s=e[$e],a=s.data[n+8],c=i==null?Kr(a)&&ng:i!=s&&(a.type&3)!==0,l=r&1&&o===a,u=id(a,s,t,c,l);return u!==null?cd(e,s,u,a,r):rr}function id(n,e,t,i,r){let o=n.providerIndexes,s=e.data,a=o&1048575,c=n.directiveStart,l=n.directiveEnd,u=o>>20,d=i?a:a+u,f=r?a+u:l;for(let h=d;h<f;h++){let m=s[h];if(h<c&&t===m||h>=c&&m.type===t)return h}if(r){let h=s[c];if(h&&Zo(h)&&h.type===t)return c}return null}function cd(n,e,t,i,r){let o=n[t],s=e.data;if(o instanceof Oc){let a=o;if(a.resolving)throw gm("");let c=ux(a.canSeeViewProviders);a.resolving=!0;let l=s[t].type||s[t],u,d=a.injectImpl?ei(a.injectImpl):null,f=Vm(n,i,0);try{o=n[t]=a.factory(void 0,r,s,n,i),e.firstCreatePass&&t>=i.directiveStart&&WT(t,s[t],e)}finally{d!==null&&ei(d),ux(c),a.resolving=!1,zm()}}return o}function rC(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(Wo)?n[Wo]:void 0;return typeof e=="number"?e>=0?e&Vx:oC:e}function dx(n,e,t){let i=1<<n;return!!(t[e+(n>>zx)]&i)}function fx(n,e){return!(n&2)&&!(n&1&&e)}var Qo=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return qx(this._tNode,this._lView,e,Vo(i),t)}};function oC(){return new Qo(Ui(),Vt())}function Hc(n){return xd(()=>{let e=n.prototype.constructor,t=e[vc]||ig(e),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let o=r[vc]||ig(r);if(o&&o!==t)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function ig(n){return am(n)?()=>{let e=ig($n(n));return e&&e()}:zo(n)}function sC(n,e,t,i,r){let o=n,s=e;for(;o!==null&&s!==null&&s[Je]&2048&&!js(s);){let a=Xx(o,s,t,i|2,rr);if(a!==rr)return a;let c=o.parent;if(!c){let l=s[Sm];if(l){let u=l.get(t,rr,i&-5);if(u!==rr)return u}c=Yx(s),s=s[qo]}o=c}return r}function Yx(n){let e=n[$e],t=e.type;return t===2?e.declTNode:t===1?n[yi]:null}function aC(){return na(Ui(),Vt())}function na(n,e){return new Vc(tr(n,e))}var Vc=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=aC}return n})();function cC(n){return n instanceof Vc?n.nativeElement:n}function lC(){return this._results[Symbol.iterator]()}var ld=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new Mn}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=x0(e);(this._changesDetected=!_0(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=lC};function Zx(n){return(n.flags&128)===128}var Ig=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Ig||{}),Kx=new Map,uC=0;function dC(){return uC++}function fC(n){Kx.set(n[Xr],n)}function rg(n){Kx.delete(n[Xr])}var hx="__ngContext__";function Ks(n,e){Zr(e)?(n[hx]=e[Xr],fC(e)):n[hx]=e}function Jx(n){return eE(n[Gs])}function Qx(n){return eE(n[vi])}function eE(n){for(;n!==null&&!Fi(n);)n=n[vi];return n}var hC;function Ag(n){hC=n}var Md=new Ve("",{factory:()=>pC}),pC="ng";var bd=new Ve(""),zc=new Ve("",{providedIn:"platform",factory:()=>"unknown"});var Sd=new Ve("",{factory:()=>ce(on).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var tE=!1,nE=new Ve("",{factory:()=>tE});var mC=(n,e,t,i)=>{};function gC(n,e,t,i){mC(n,e,t,i)}function Rg(n){return(n.flags&32)===32}var vC=()=>null;function iE(n,e,t=!1){return vC(n,e,t)}function rE(n,e){let t=n.contentQueries;if(t!==null){let i=Ke(null);try{for(let r=0;r<t.length;r+=2){let o=t[r],s=t[r+1];if(s!==-1){let a=n.data[s];Yu(o),a.contentQueries(2,e[s],s)}}}finally{Ke(i)}}}function og(n,e,t){Yu(0);let i=Ke(null);try{e(n,t)}finally{Ke(i)}}function oE(n,e,t){if(wm(e)){let i=Ke(null);try{let r=e.directiveStart,o=e.directiveEnd;for(let s=r;s<o;s++){let a=n.data[s];if(a.contentQueries){let c=t[s];a.contentQueries(1,c,s)}}}finally{Ke(i)}}}var Hi=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(Hi||{});var sg=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${om})`}};function sE(n){return n instanceof sg?n.changingThisBreaksApplicationSecurity:n}function yC(n,e){return n.createText(e)}function _C(n,e,t){n.setValue(e,t)}function aE(n,e,t){return n.createElement(e,t)}function ud(n,e,t,i,r){n.insertBefore(e,t,i,r)}function cE(n,e,t){n.appendChild(e,t)}function px(n,e,t,i,r){i!==null?ud(n,e,t,i,r):cE(n,e,t)}function xC(n,e,t,i){n.removeChild(null,e,t,i)}function EC(n,e,t){n.setAttribute(e,"style",t)}function MC(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function lE(n,e,t){let{mergedAttrs:i,classes:r,styles:o}=t;i!==null&&YT(n,e,i),r!==null&&MC(n,e,r),o!==null&&EC(n,e,o)}function uE(n){return n instanceof Function?n():n}function bC(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let o=e.length;if(r+o===i||n.charCodeAt(r+o)<=32)return r}t=r+1}}var dE="ng-template";function SC(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&bC(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Dg(n))return!1;if(r=e.indexOf(1,r),r>-1){let o;for(;++r<e.length&&typeof(o=e[r])=="string";)if(o.toLowerCase()===t)return!0}return!1}function Dg(n){return n.type===4&&n.value!==dE}function wC(n,e,t){let i=n.type===4&&!t?dE:n.value;return e===i}function TC(n,e,t){let i=4,r=n.attrs,o=r!==null?AC(r):0,s=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!s&&!Bi(i)&&!Bi(c))return!1;if(s&&Bi(c))continue;s=!1,i=c|i&1;continue}if(!s)if(i&4){if(i=2|i&1,c!==""&&!wC(n,c,t)||c===""&&e.length===1){if(Bi(i))return!1;s=!0}}else if(i&8){if(r===null||!SC(n,r,c,t)){if(Bi(i))return!1;s=!0}}else{let l=e[++a],u=CC(c,r,Dg(n),t);if(u===-1){if(Bi(i))return!1;s=!0;continue}if(l!==""){let d;if(u>o?d="":d=r[u+1].toLowerCase(),i&2&&l!==d){if(Bi(i))return!1;s=!0}}}}return Bi(i)||s}function Bi(n){return(n&1)===0}function CC(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let o=!1;for(;r<e.length;){let s=e[r];if(s===n)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return RC(e,n)}function IC(n,e,t=!1){for(let i=0;i<e.length;i++)if(TC(n,e[i],t))return!0;return!1}function AC(n){for(let e=0;e<n.length;e++){let t=n[e];if(ZT(t))return e}return n.length}function RC(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function mx(n,e){return n?":not("+e.trim()+")":e}function DC(n){let e=n[0],t=1,i=2,r="",o=!1;for(;t<n.length;){let s=n[t];if(typeof s=="string")if(i&2){let a=n[++t];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!Bi(s)&&(e+=mx(o,r),r=""),i=s,o=o||!Bi(i);t++}return r!==""&&(e+=mx(o,r)),e}function NC(n){return n.map(DC).join(",")}function PC(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let o=n[i];if(typeof o=="string")r===2?o!==""&&e.push(o,n[++i]):r===8&&t.push(o);else{if(!Bi(r))break;r=o}i++}return t.length&&e.push(1,...t),e}var Tr={};function Ng(n,e,t,i,r,o,s,a,c,l,u){let d=Rn+i,f=d+r,h=LC(d,f),m=typeof l=="function"?l():l;return h[$e]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,d),bindingStartIndex:d,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:c,consts:m,incompleteFirstPass:!1,ssrId:u}}function LC(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:Tr);return t}function OC(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=Ng(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function Pg(n,e,t,i,r,o,s,a,c,l,u){let d=e.blueprint.slice();return d[Oi]=r,d[Je]=i|4|128|8|64|1024,(l!==null||n&&n[Je]&2048)&&(d[Je]|=2048),Im(d),d[pn]=d[qo]=n,d[wn]=t,d[Qi]=s||n&&n[Qi],d[mn]=a||n&&n[mn],d[$r]=c||n&&n[$r]||null,d[yi]=o,d[Xr]=dC(),d[Cc]=u,d[Sm]=l,d[_i]=e.type==2?n[_i]:d,d}function FC(n,e,t){let i=tr(e,n),r=OC(t),o=n[Qi].rendererFactory,s=Lg(n,Pg(n,r,null,fE(t),i,e,null,o.createRenderer(i,t),null,null,null));return n[e.index]=s}function fE(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function hE(n,e,t,i){if(t===0)return-1;let r=e.length;for(let o=0;o<t;o++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Lg(n,e){return n[Gs]?n[bm][vi]=e:n[Gs]=e,n[bm]=e,e}function Kt(n=1){pE(ki(),Vt(),Jo()+n,!1)}function pE(n,e,t,i){if(!i)if((e[Je]&3)===3){let o=n.preOrderCheckHooks;o!==null&&td(e,o,t)}else{let o=n.preOrderHooks;o!==null&&nd(e,o,0,t)}Qr(t)}var wd=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(wd||{});function ag(n,e,t,i){let r=Ke(null);try{let[o,s,a]=n.inputs[t],c=null;(s&wd.SignalBased)!==0&&(c=e[o][Qn]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,o):Ox(e,c,o,i)}finally{Ke(r)}}var wr=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(wr||{}),kC;function Og(n,e){return kC(n,e)}var zz=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var cg=new WeakMap,lg=new WeakSet;function UC(n,e){let t=cg.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let o=t.length-1;o>=0;o--){let s=t[o],a=s.parentNode;s===e?(t.splice(o,1),lg.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(t.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function BC(n,e){let t=cg.get(n);t?t.includes(e)||t.push(e):cg.set(n,[e])}var Js=new Set,Fg=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Fg||{}),ia=new Ve(""),gx=new Set;function ra(n){gx.has(n)||(gx.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var mE=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=je({token:n,providedIn:"root",factory:()=>new n})}return n})();var HC=new Ve("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:ce(rn)})});function gE(n,e,t){let i=n.get(HC);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function VC(n,e){for(let[t,i]of e)gE(n,i.animateFns)}function vx(n,e,t,i){let r=n?.[Ws]?.enter;e!==null&&r&&r.has(t.index)&&VC(i,r)}function Ys(n,e,t,i,r,o,s,a){if(r!=null){let c,l=!1;Fi(r)?c=r:Zr(r)&&(l=!0,r=r[Oi]);let u=xi(r);n===0&&i!==null?(vx(a,i,o,t),s==null?cE(e,i,u):ud(e,i,u,s||null,!0)):n===1&&i!==null?(vx(a,i,o,t),ud(e,i,u,s||null,!0),UC(o,u)):n===2?(a?.[Ws]?.leave?.has(o.index)&&BC(o,u),yx(a,o,t,d=>{if(lg.has(u)){lg.delete(u);return}xC(e,u,l,d)})):n===3&&yx(a,o,t,()=>{e.destroyNode(u)}),c!=null&&eI(e,n,t,c,o,i,s)}}function zC(n,e){vE(n,e),e[Oi]=null,e[yi]=null}function GC(n,e,t,i,r,o){i[Oi]=r,i[yi]=e,Td(n,i,t,1,r,o)}function vE(n,e){e[Qi].changeDetectionScheduler?.notify(9),Td(n,e,e[mn],2,null,null)}function WC(n){let e=n[Gs];if(!e)return Jm(n[$e],n);for(;e;){let t=null;if(Zr(e))t=e[Gs];else{let i=e[Dn];i&&(t=i)}if(!t){for(;e&&!e[vi]&&e!==n;)Zr(e)&&Jm(e[$e],e),e=e[pn];e===null&&(e=n),Zr(e)&&Jm(e[$e],e),t=e&&e[vi]}e=t}}function kg(n,e){let t=n[Yo],i=t.indexOf(e);t.splice(i,1)}function Ug(n,e){if(Ko(e))return;let t=e[mn];t.destroyNode&&Td(n,e,t,3,null,null),WC(e)}function Jm(n,e){if(Ko(e))return;let t=Ke(null);try{e[Je]&=-129,e[Je]|=256,e[ni]&&uc(e[ni]),qC(n,e),$C(n,e),e[$e].type===1&&e[mn].destroy();let i=e[qr];if(i!==null&&Fi(e[pn])){i!==e[pn]&&kg(i,e);let r=e[er];r!==null&&r.detachView(n)}rg(e)}finally{Ke(t)}}function yx(n,e,t,i){let r=n?.[Ws];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&Js.add(n[Xr]),gE(t,()=>{if(r.leave&&r.leave.has(e.index)){let s=r.leave.get(e.index),a=[];if(s){for(let c=0;c<s.animateFns.length;c++){let l=s.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),jC(n,i)}else n&&Js.delete(n[Xr]),i(!1)},r)}function jC(n,e){let t=n[Ws]?.running;if(t){t.then(()=>{n[Ws].running=void 0,Js.delete(n[Xr]),e(!0)});return}e(!1)}function $C(n,e){let t=n.cleanup,i=e[zs];if(t!==null)for(let s=0;s<t.length-1;s+=2)if(typeof t[s]=="string"){let a=t[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[t[s+1]];t[s].call(a)}i!==null&&(e[zs]=null);let r=e[br];if(r!==null){e[br]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=e[Ic];if(o!==null){e[Ic]=null;for(let s of o)s.destroy()}}function qC(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof Oc)){let o=t[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],c=o[s+1];Pt(bt.LifecycleHookStart,a,c);try{c.call(a)}finally{Pt(bt.LifecycleHookEnd,a,c)}}else{Pt(bt.LifecycleHookStart,r,o);try{o.call(r)}finally{Pt(bt.LifecycleHookEnd,r,o)}}}}}function XC(n,e,t){return YC(n,e.parent,t)}function YC(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[Oi];if(Kr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===Hi.None||r===Hi.Emulated)return null}return tr(i,t)}function ZC(n,e,t){return JC(n,e,t)}function KC(n,e,t){return n.type&40?tr(n,t):null}var JC=KC,_x;function Bg(n,e,t,i){let r=XC(n,i,e),o=e[mn],s=i.parent||e[yi],a=ZC(s,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)px(o,r,t[c],a,!1);else px(o,r,t,a,!1);_x!==void 0&&_x(o,i,e,t,r)}function Pc(n,e){if(e!==null){let t=e.type;if(t&3)return tr(e,n);if(t&4)return ug(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return Pc(n,i);{let r=n[e.index];return Fi(r)?ug(-1,r):xi(r)}}else{if(t&128)return Pc(n,e.next);if(t&32)return Og(e,n)()||xi(n[e.index]);{let i=yE(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Wr(n[_i]);return Pc(r,i)}else return Pc(n,e.next)}}}return null}function yE(n,e){if(e!==null){let i=n[_i][yi],r=e.projection;return i.projection[r]}return null}function ug(n,e){let t=Dn+n+1;if(t<e.length){let i=e[t],r=i[$e].firstChild;if(r!==null)return Pc(i,r)}return e[Yr]}function Hg(n,e,t,i,r,o,s){for(;t!=null;){let a=i[$r];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(s&&e===0&&(c&&Ks(xi(c),i),t.flags|=2),!Rg(t))if(l&8)Hg(n,e,t.child,i,r,o,!1),Ys(e,n,a,r,c,t,o,i);else if(l&32){let u=Og(t,i),d;for(;d=u();)Ys(e,n,a,r,d,t,o,i);Ys(e,n,a,r,c,t,o,i)}else l&16?QC(n,e,i,t,r,o):Ys(e,n,a,r,c,t,o,i);t=s?t.projectionNext:t.next}}function Td(n,e,t,i,r,o){Hg(t,i,n.firstChild,e,r,o,!1)}function QC(n,e,t,i,r,o){let s=t[_i],c=s[yi].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];Ys(e,n,t[$r],r,u,i,o,t)}else{let l=c,u=s[pn];Zx(i)&&(l.flags|=128),Hg(n,e,l,u,r,o,!0)}}function eI(n,e,t,i,r,o,s){let a=i[Yr],c=xi(i);a!==c&&Ys(e,n,t,o,a,r,s);for(let l=Dn;l<i.length;l++){let u=i[l];Td(u[$e],u,n,e,o,a)}}function tI(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let o=i.indexOf("-")===-1?void 0:wr.DashCase;r==null?n.removeStyle(t,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=wr.Important),n.setStyle(t,i,r,o))}}function _E(n,e,t,i,r){let o=Jo(),s=i&2;try{Qr(-1),s&&e.length>Rn&&pE(n,e,Rn,!1);let a=s?bt.TemplateUpdateStart:bt.TemplateCreateStart;Pt(a,r,t),t(i,r)}finally{Qr(o);let a=s?bt.TemplateUpdateEnd:bt.TemplateCreateEnd;Pt(a,r,t)}}function xE(n,e,t){sI(n,e,t),(t.flags&64)===64&&aI(n,e,t)}function Vg(n,e,t=tr){let i=e.localNames;if(i!==null){let r=e.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?t(e,n):n[s];n[r++]=a}}}function nI(n,e,t,i){let o=i.get(nE,tE)||t===Hi.ShadowDom||t===Hi.ExperimentalIsolatedShadowDom,s=n.selectRootElement(e,o);return iI(s),s}function iI(n){rI(n)}var rI=()=>null;function oI(n,e,t,i,r,o){if(n.type&3){let s=tr(n,e);i=o!=null?o(i,n.value||"",t):i,r.setProperty(s,t,i)}else n.type&12}function sI(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Kr(t)&&FC(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Gx(t,e);let o=t.initialInputs;for(let s=i;s<r;s++){let a=n.data[s],c=cd(e,n,s,t);if(Ks(c,e),o!==null&&uI(e,s-i,c,a,t,o),Zo(a)){let l=nr(t.index,e);l[wn]=cd(e,n,s,t)}}}function aI(n,e,t){let i=t.directiveStart,r=t.directiveEnd,o=t.index,s=$0();try{Qr(o);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Xu(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&cI(c,l)}}finally{Qr(-1),Xu(s)}}function cI(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function lI(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let o=t[r];IC(e,o.selectors,!1)&&(i??=[],Zo(o)?i.unshift(o):i.push(o))}return i}function uI(n,e,t,i,r,o){let s=o[e];if(s!==null)for(let a=0;a<s.length;a+=2){let c=s[a],l=s[a+1];ag(i,t,c,l)}}function EE(n,e,t,i,r){let o=Rn+t,s=e[$e],a=r(s,e,n,i,t);e[o]=a,Xs(n,!0);let c=n.type===2;return c?(lE(e[mn],a,n),(F0()===0||Gu(n))&&Ks(a,e),k0()):Ks(a,e),Ju()&&(!c||!Rg(n))&&Bg(s,e,a,n),n}function ME(n){let e=n;return km()?V0():(e=e.parent,Xs(e,!1)),e}function dI(n,e){let t=n[$r];if(!t)return;let i;try{i=t.get(ir,null)}catch{i=null}i?.(e)}function bE(n,e,t,i,r){let o=n.inputs?.[i],s=n.hostDirectiveInputs?.[i],a=!1;if(s)for(let c=0;c<s.length;c+=2){let l=s[c],u=s[c+1],d=e.data[l];ag(d,t[l],u,r),a=!0}if(o)for(let c of o){let l=t[c],u=e.data[c];ag(u,l,i,r),a=!0}return a}function fI(n,e){let t=nr(e,n),i=t[$e];hI(i,t);let r=t[Oi];r!==null&&t[Cc]===null&&(t[Cc]=iE(r,t[$r])),Pt(bt.ComponentStart);try{zg(i,t,t[wn])}finally{Pt(bt.ComponentEnd,t[wn])}}function hI(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function zg(n,e,t){Zu(e);try{let i=n.viewQuery;i!==null&&og(1,i,t);let r=n.template;r!==null&&_E(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[er]?.finishViewCreation(n),n.staticContentQueries&&rE(n,e),n.staticViewQueries&&og(2,n.viewQuery,t);let o=n.components;o!==null&&pI(e,o)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Je]&=-5,Ku()}}function pI(n,e){for(let t=0;t<e.length;t++)fI(n,e[t])}function SE(n,e,t,i){let r=Ke(null);try{let o=e.tView,a=n[Je]&4096?4096:16,c=Pg(n,o,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[qr]=l;let u=n[er];return u!==null&&(c[er]=u.createEmbeddedView(o)),zg(o,c,t),c}finally{Ke(r)}}function dg(n,e){return!e||e.firstChild===null||Zx(n)}function Fc(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let o=e[t.index];o!==null&&i.push(xi(o)),Fi(o)&&wE(o,i);let s=t.type;if(s&8)Fc(n,e,t.child,i);else if(s&32){let a=Og(t,e),c;for(;c=a();)i.push(c)}else if(s&16){let a=yE(e,t);if(Array.isArray(a))i.push(...a);else{let c=Wr(e[_i]);Fc(c[$e],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function wE(n,e){for(let t=Dn;t<n.length;t++){let i=n[t],r=i[$e].firstChild;r!==null&&Fc(i[$e],i,r,e)}n[Yr]!==n[Oi]&&e.push(n[Yr])}function TE(n){if(n[zu]!==null){for(let e of n[zu])e.impl.addSequence(e);n[zu].length=0}}var CE=[];function mI(n){return n[ni]??gI(n)}function gI(n){let e=CE.pop()??Object.create(yI);return e.lView=n,e}function vI(n){n.lView[ni]!==n&&(n.lView=null,CE.push(n))}var yI=Ut(Ee({},ac),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Dc(n.lView)},consumerOnSignalRead(){this.lView[ni]=this}});function _I(n){let e=n[ni]??Object.create(xI);return e.lView=n,e}var xI=Ut(Ee({},ac),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Wr(n.lView);for(;e&&!IE(e[$e]);)e=Wr(e);e&&Am(e)},consumerOnSignalRead(){this.lView[ni]=this}});function IE(n){return n.type!==2}function AE(n){if(n[Ic]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Ic])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Je]&8192)}}var EI=100;function RE(n,e=0){let i=n[Qi].rendererFactory,r=!1;r||i.begin?.();try{MI(n,e)}finally{r||i.end?.()}}function MI(n,e){let t=Um();try{Bm(!0),fg(n,e);let i=0;for(;Rc(n);){if(i===EI)throw new Oe(103,!1);i++,fg(n,1)}}finally{Bm(t)}}function bI(n,e,t,i){if(Ko(e))return;let r=e[Je],o=!1,s=!1;Zu(e);let a=!0,c=null,l=null;o||(IE(n)?(l=mI(e),c=lc(l)):ou()===null?(a=!1,l=_I(e),c=lc(l)):e[ni]&&(uc(e[ni]),e[ni]=null));try{Im(e),z0(n.bindingStartIndex),t!==null&&_E(n,e,t,2,i);let u=(r&3)===3;if(!o)if(u){let h=n.preOrderCheckHooks;h!==null&&td(e,h,null)}else{let h=n.preOrderHooks;h!==null&&nd(e,h,0,null),Zm(e,0)}if(s||SI(e),AE(e),DE(e,0),n.contentQueries!==null&&rE(n,e),!o)if(u){let h=n.contentCheckHooks;h!==null&&td(e,h)}else{let h=n.contentHooks;h!==null&&nd(e,h,1),Zm(e,1)}TI(n,e);let d=n.components;d!==null&&PE(e,d,0);let f=n.viewQuery;if(f!==null&&og(2,f,i),!o)if(u){let h=n.viewCheckHooks;h!==null&&td(e,h)}else{let h=n.viewHooks;h!==null&&nd(e,h,2),Zm(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Vu]){for(let h of e[Vu])h();e[Vu]=null}o||(TE(e),e[Je]&=-73)}catch(u){throw o||Dc(e),u}finally{l!==null&&(su(l,c),a&&vI(l)),Ku()}}function DE(n,e){for(let t=Jx(n);t!==null;t=Qx(t))for(let i=Dn;i<t.length;i++){let r=t[i];NE(r,e)}}function SI(n){for(let e=Jx(n);e!==null;e=Qx(e)){if(!(e[Je]&2))continue;let t=e[Yo];for(let i=0;i<t.length;i++){let r=t[i];Am(r)}}}function wI(n,e,t){Pt(bt.ComponentStart);let i=nr(e,n);try{NE(i,t)}finally{Pt(bt.ComponentEnd,i[wn])}}function NE(n,e){ju(n)&&fg(n,e)}function fg(n,e){let i=n[$e],r=n[Je],o=n[ni],s=!!(e===0&&r&16);if(s||=!!(r&64&&e===0),s||=!!(r&1024),s||=!!(o?.dirty&&au(o)),s||=!1,o&&(o.dirty=!1),n[Je]&=-9217,s)bI(i,n,i.template,n[wn]);else if(r&8192){let a=Ke(null);try{AE(n),DE(n,1);let c=i.components;c!==null&&PE(n,c,1),TE(n)}finally{Ke(a)}}}function PE(n,e,t){for(let i=0;i<e.length;i++)wI(n,e[i],t)}function TI(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Qr(~r);else{let o=r,s=t[++i],a=t[++i];j0(s,o);let c=e[o];Pt(bt.HostBindingsUpdateStart,c);try{a(2,c)}finally{Pt(bt.HostBindingsUpdateEnd,c)}}}}finally{Qr(-1)}}function Gg(n,e){let t=Um()?64:1088;for(n[Qi].changeDetectionScheduler?.notify(e);n;){n[Je]|=t;let i=Wr(n);if(js(n)&&!i)return n;n=i}return null}function LE(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function CI(n,e){let t=Dn+e;if(t<n.length)return n[t]}function OE(n,e,t,i=!0){let r=e[$e];if(AI(r,e,n,t),i){let s=ug(t,n),a=e[mn],c=a.parentNode(n[Yr]);c!==null&&GC(r,n[yi],a,e,c,s)}let o=e[Cc];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function II(n,e){let t=dd(n,e);return t!==void 0&&Ug(t[$e],t),t}function dd(n,e){if(n.length<=Dn)return;let t=Dn+e,i=n[t];if(i){let r=i[qr];r!==null&&r!==n&&kg(r,i),e>0&&(n[t-1][vi]=i[vi]);let o=Sc(n,Dn+e);zC(i[$e],i);let s=o[er];s!==null&&s.detachView(o[$e]),i[pn]=null,i[vi]=null,i[Je]&=-129}return i}function AI(n,e,t,i){let r=Dn+i,o=t.length;i>0&&(t[r-1][vi]=e),i<o-Dn?(e[vi]=t[r],ym(t,Dn+i,e)):(t.push(e),e[vi]=null),e[pn]=t;let s=e[qr];s!==null&&t!==s&&FE(s,e);let a=e[er];a!==null&&a.insertView(n),$u(e),e[Je]|=128}function FE(n,e){let t=n[Yo],i=e[pn];if(Zr(i))n[Je]|=2;else{let r=i[pn][_i];e[_i]!==r&&(n[Je]|=2)}t===null?n[Yo]=[e]:t.push(e)}var no=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[$e];return Fc(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[wn]}set context(e){this._lView[wn]=e}get destroyed(){return Ko(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[pn];if(Fi(e)){let t=e[Ac],i=t?t.indexOf(this):-1;i>-1&&(dd(e,i),Sc(t,i))}this._attachedToViewContainer=!1}Ug(this._lView[$e],this._lView)}onDestroy(e){Rm(this._lView,e)}markForCheck(){Gg(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Je]&=-129}reattach(){$u(this._lView),this._lView[Je]|=128}detectChanges(){this._lView[Je]|=1024,RE(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Oe(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=js(this._lView),t=this._lView[qr];t!==null&&!e&&kg(t,this._lView),vE(this._lView[$e],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Oe(902,!1);this._appRef=e;let t=js(this._lView),i=this._lView[qr];i!==null&&!t&&FE(i,this._lView),$u(this._lView)}};var kc=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=RI;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let o=SE(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new no(o)}}return n})();function RI(){return Wg(Ui(),Vt())}function Wg(n,e){return n.type&4?new kc(e,n,na(n,e)):null}function Cd(n,e,t,i,r){let o=n.data[e];if(o===null)o=DI(n,e,t,i,r),W0()&&(o.flags|=32);else if(o.type&64){o.type=t,o.value=i,o.attrs=r;let s=H0();o.injectorIndex=s===null?-1:s.injectorIndex}return Xs(o,!0),o}function DI(n,e,t,i,r){let o=Fm(),s=km(),a=s?o:o&&o.parent,c=n.data[e]=PI(n,a,t,e,i,r);return NI(n,c,o,s),c}function NI(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function PI(n,e,t,i,r,o){let s=e?e.injectorIndex:-1,a=0;return B0()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var LI=()=>null,OI=()=>null;function xx(n,e){return LI(n,e)}function FI(n,e,t){return OI(n,e,t)}var kE=class{},Id=class{},hg=class{resolveComponentFactory(e){throw new Oe(917,!1)}},Gc=class{static NULL=new hg},es=class{};var UE=(()=>{class n{static \u0275prov=je({token:n,providedIn:"root",factory:()=>null})}return n})();var rd={},pg=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,rd,i);return r!==rd||t===rd?r:this.parentInjector.get(e,t,i)}};function fd(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,o=0;if(e!==null)for(let s=0;s<e.length;s++){let a=e[s];if(typeof a=="number")o=a;else if(o==1)r=sm(r,a);else if(o==2){let c=a,l=e[++s];i=sm(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function BE(n,e=0){let t=Vt();if(t===null)return it(n,e);let i=Ui();return qx(i,t,$n(n),e)}function kI(n,e,t,i,r){let o=i===null?null:{"":-1},s=r(n,t);if(s!==null){let a=s,c=null,l=null;for(let u of s)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(s);break}HI(n,e,t,a,o,c,l)}o!==null&&i!==null&&UI(t,i,o)}function UI(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let o=t[e[r+1]];if(o==null)throw new Oe(-301,!1);i.push(e[r],o)}}function BI(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function HI(n,e,t,i,r,o,s){let a=i.length,c=null;for(let f=0;f<a;f++){let h=i[f];c===null&&Zo(h)&&(c=h,BI(n,t,f)),nC(Gx(t,e),n,h.type)}$I(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,d=hE(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(t.mergedAttrs=Ed(t.mergedAttrs,h.hostAttrs),zI(n,t,e,d,h),jI(d,h,r),s!==null&&s.has(h)){let[y,g]=s.get(h);t.directiveToIndex.set(h.type,[d,y+t.directiveStart,g+t.directiveStart])}else(o===null||!o.has(h))&&t.directiveToIndex.set(h.type,d);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let m=h.type.prototype;!l&&(m.ngOnChanges||m.ngOnInit||m.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(m.ngOnChanges||m.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),d++}VI(n,t,o)}function VI(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))Ex(0,e,r,i),Ex(1,e,r,i),bx(e,i,!1);else{let o=t.get(r);Mx(0,e,o,i),Mx(1,e,o,i),bx(e,i,!0)}}}function Ex(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;n===0?s=e.inputs??={}:s=e.outputs??={},s[o]??=[],s[o].push(i),HE(e,o)}}function Mx(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),HE(e,s)}}function HE(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function bx(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:o}=n;if(i===null||!t&&r===null||t&&o===null||Dg(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){s??=[],s.push(c,i[a+1]);break}}else if(t&&o.hasOwnProperty(c)){let l=o[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){s??=[],s.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(s)}function zI(n,e,t,i,r){n.data[i]=r;let o=r.factory||(r.factory=zo(r.type,!0)),s=new Oc(o,Zo(r),BE,null);n.blueprint[i]=s,t[i]=s,GI(n,e,i,hE(n,t,r.hostVars,Tr),r)}function GI(n,e,t,i,r){let o=r.hostBindings;if(o){let s=n.hostBindingOpCodes;s===null&&(s=n.hostBindingOpCodes=[]);let a=~e.index;WI(s)!=a&&s.push(a),s.push(t,i,o)}}function WI(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function jI(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;Zo(e)&&(t[""]=n)}}function $I(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function VE(n,e,t,i,r,o,s,a){let c=e[$e],l=c.consts,u=Jr(l,s),d=Cd(c,n,t,i,u);return o&&kI(c,e,d,Jr(l,a),r),d.mergedAttrs=Ed(d.mergedAttrs,d.attrs),d.attrs!==null&&fd(d,d.attrs,!1),d.mergedAttrs!==null&&fd(d,d.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,d),d}function zE(n,e){jT(n,e),wm(e)&&n.queries.elementEnd(e)}function qI(n,e,t,i,r,o){let s=e.consts,a=Jr(s,r),c=Cd(e,n,t,i,a);if(c.mergedAttrs=Ed(c.mergedAttrs,c.attrs),o!=null){let l=Jr(s,o);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&fd(c,c.attrs,!1),c.mergedAttrs!==null&&fd(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function Ad(n,e,t){if(t===Tr)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}function XI(n,e,t){return function i(r){let o=Kr(n)?nr(n.index,e):e;Gg(o,5);let s=e[wn],a=Sx(e,s,t,r),c=i.__ngNextListenerFn__;for(;c;)a=Sx(e,s,c,r)&&a,c=c.__ngNextListenerFn__;return a}}function Sx(n,e,t,i){let r=Ke(null);try{return Pt(bt.OutputStart,e,t),t(i)!==!1}catch(o){return dI(n,o),!1}finally{Pt(bt.OutputEnd,e,t),Ke(r)}}function YI(n,e,t,i,r,o,s,a){let c=Gu(n),l=!1,u=null;if(!i&&c&&(u=KI(e,t,o,n.index)),u!==null){let d=u.__ngLastListenerFn__||u;d.__ngNextListenerFn__=s,u.__ngLastListenerFn__=s,l=!0}else{let d=tr(n,t),f=i?i(d):d;gC(t,f,o,a);let h=r.listen(f,o,a);if(!ZI(o)){let m=i?y=>i(xi(y[n.index])):n.index;JI(m,e,t,o,a,h,!1)}}return l}function ZI(n){return n.startsWith("animation")||n.startsWith("transition")}function KI(n,e,t,i){let r=n.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===t&&r[o+1]===i){let a=e[zs],c=r[o+2];return a&&a.length>c?a[c]:null}typeof s=="string"&&(o+=2)}return null}function JI(n,e,t,i,r,o,s){let a=e.firstCreatePass?Nm(e):null,c=Dm(t),l=c.length;c.push(r,o),a&&a.push(i,n,l,(l+1)*(s?-1:1))}var mg=Symbol("BINDING");function GE(n){return n.debugInfo?.className||n.type.name||null}var hd=class extends Gc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=jr(e);return new Qs(t,this.ngModule)}};function QI(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],o={propName:t,templateName:e,isSignal:(i&wd.SignalBased)!==0};return r&&(o.transform=r),o})}function eA(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function tA(n,e,t){let i=e instanceof rn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new pg(t,i):t}function nA(n){let e=n.get(es,null);if(e===null)throw new Oe(407,!1);let t=n.get(UE,null),i=n.get(Bs,null),r=n.get(ia,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function iA(n,e){let t=WE(n);return aE(e,t,t==="svg"?Tm:t==="math"?R0:null)}function WE(n){return(n.selectors[0][0]||"div").toLowerCase()}var Qs=class extends Id{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=QI(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=eA(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=NC(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,o,s){Pt(bt.DynamicComponentStart);let a=Ke(null);try{let c=this.componentDef,l=tA(c,r||this.ngModule,e),u=nA(l),d=u.tracingService;return d&&d.componentCreate?d.componentCreate(GE(c),()=>this.createComponentRef(u,l,t,i,o,s)):this.createComponentRef(u,l,t,i,o,s)}finally{Ke(a)}}createComponentRef(e,t,i,r,o,s){let a=this.componentDef,c=rA(r,a,s,o),l=e.rendererFactory.createRenderer(null,a),u=r?nI(l,r,a.encapsulation,t):iA(a,l),d=s?.some(wx)||o?.some(m=>typeof m!="function"&&m.bindings.some(wx)),f=Pg(null,c,null,512|fE(a),null,null,e,l,t,null,iE(u,t,!0));f[Rn]=u,Zu(f);let h=null;try{let m=VE(Rn,f,2,"#host",()=>c.directiveRegistry,!0,0);lE(l,u,m),Ks(u,f),xE(c,f,m),oE(c,m,f),zE(c,m),i!==void 0&&sA(m,this.ngContentSelectors,i),h=nr(m.index,f),f[wn]=h[wn],zg(c,f,null)}catch(m){throw h!==null&&rg(h),rg(f),m}finally{Pt(bt.DynamicComponentEnd),Ku()}return new pd(this.componentType,f,!!d)}};function rA(n,e,t,i){let r=n?["ng-version","21.2.5"]:PC(e.selectors[0]),o=null,s=null,a=0;if(t)for(let u of t)a+=u[mg].requiredVars,u.create&&(u.targetIdx=0,(o??=[]).push(u)),u.update&&(u.targetIdx=0,(s??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let d=i[u];if(typeof d!="function")for(let f of d.bindings){a+=f[mg].requiredVars;let h=u+1;f.create&&(f.targetIdx=h,(o??=[]).push(f)),f.update&&(f.targetIdx=h,(s??=[]).push(f))}}let c=[e];if(i)for(let u of i){let d=typeof u=="function"?u:u.type,f=mm(d);c.push(f)}return Ng(0,null,oA(o,s),1,a,c,null,null,null,[r],null)}function oA(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function wx(n){let e=n[mg].kind;return e==="input"||e==="twoWay"}var pd=class extends kE{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Wu(t[$e],Rn),this.location=na(this._tNode,t),this.instance=nr(this._tNode.index,t)[wn],this.hostView=this.changeDetectorRef=new no(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,o=bE(i,r[$e],r,e,t);this.previousInputValues.set(e,t);let s=nr(i.index,r);Gg(s,1)}get injector(){return new Qo(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function sA(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let o=t[r];i.push(o!=null&&o.length?Array.from(o):null)}}var oa=(()=>{class n{static __NG_ELEMENT_ID__=aA}return n})();function aA(){let n=Ui();return jE(n,Vt())}var gg=class n extends oa{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return na(this._hostTNode,this._hostLView)}get injector(){return new Qo(this._hostTNode,this._hostLView)}get parentInjector(){let e=Cg(this._hostTNode,this._hostLView);if(Hx(e)){let t=ad(e,this._hostLView),i=sd(e),r=t[$e].data[i+8];return new Qo(r,t)}else return new Qo(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=Tx(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-Dn}createEmbeddedView(e,t,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=xx(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},o,s);return this.insertImpl(a,r,dg(this._hostTNode,s)),a}createComponent(e,t,i,r,o,s,a){let c=e&&!HT(e),l;if(c)l=t;else{let g=t||{};l=g.index,i=g.injector,r=g.projectableNodes,o=g.environmentInjector||g.ngModuleRef,s=g.directives,a=g.bindings}let u=c?e:new Qs(jr(e)),d=i||this.parentInjector;if(!o&&u.ngModule==null){let p=(c?d:this.parentInjector).get(rn,null);p&&(o=p)}let f=jr(u.componentType??{}),h=xx(this._lContainer,f?.id??null),m=h?.firstChild??null,y=u.create(d,r,m,o,s,a);return this.insertImpl(y.hostView,l,dg(this._hostTNode,h)),y}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(N0(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[pn],l=new n(c,c[yi],c[pn]);l.detach(l.indexOf(e))}}let o=this._adjustIndex(t),s=this._lContainer;return OE(s,r,o,i),e.attachToViewContainerRef(),ym(Qm(s),o,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=Tx(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=dd(this._lContainer,t);i&&(Sc(Qm(this._lContainer),t),Ug(i[$e],i))}detach(e){let t=this._adjustIndex(e,-1),i=dd(this._lContainer,t);return i&&Sc(Qm(this._lContainer),t)!=null?new no(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function Tx(n){return n[Ac]}function Qm(n){return n[Ac]||(n[Ac]=[])}function jE(n,e){let t,i=e[n.index];return Fi(i)?t=i:(t=LE(i,e,null,n),e[n.index]=t,Lg(e,t)),lA(t,e,n,i),new gg(t,n,e)}function cA(n,e){let t=n[mn],i=t.createComment(""),r=tr(e,n),o=t.parentNode(r);return ud(t,o,i,t.nextSibling(r),!1),i}var lA=fA,uA=()=>!1;function dA(n,e,t){return uA(n,e,t)}function fA(n,e,t,i){if(n[Yr])return;let r;t.type&8?r=xi(i):r=cA(e,t),n[Yr]=r}var vg=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},yg=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let o=0;o<i;o++){let s=t.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)jg(e,t).matches!==null&&this.queries[t].setDirty()}},_g=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=xA(e):this.predicate=e}},xg=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,o=this.getByIndex(i).embeddedTView(e,r);o&&(o.indexInDeclarationView=i,t!==null?t.push(o):t=[o])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},Eg=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(e,t,hA(t,o)),this.matchTNodeWithReadOption(e,t,id(t,e,o,!1,!1))}else i===kc?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,id(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===Vc||r===oa||r===kc&&t.type&4)this.addMatch(t.index,-2);else{let o=id(t,e,r,!1,!1);o!==null&&this.addMatch(t.index,o)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function hA(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function pA(n,e){return n.type&11?na(n,e):n.type&4?Wg(n,e):null}function mA(n,e,t,i){return t===-1?pA(e,n):t===-2?gA(n,e,i):cd(n,n[$e],t,e)}function gA(n,e,t){if(t===Vc)return na(e,n);if(t===kc)return Wg(e,n);if(t===oa)return jE(e,n)}function $E(n,e,t,i){let r=e[er].queries[i];if(r.matches===null){let o=n.data,s=t.matches,a=[];for(let c=0;s!==null&&c<s.length;c+=2){let l=s[c];if(l<0)a.push(null);else{let u=o[l];a.push(mA(e,u,s[c+1],t.metadata.read))}}r.matches=a}return r.matches}function Mg(n,e,t,i){let r=n.queries.getByIndex(t),o=r.matches;if(o!==null){let s=$E(n,e,r,t);for(let a=0;a<o.length;a+=2){let c=o[a];if(c>0)i.push(s[a/2]);else{let l=o[a+1],u=e[-c];for(let d=Dn;d<u.length;d++){let f=u[d];f[qr]===f[pn]&&Mg(f[$e],f,l,i)}if(u[Yo]!==null){let d=u[Yo];for(let f=0;f<d.length;f++){let h=d[f];Mg(h[$e],h,l,i)}}}}}return i}function vA(n,e){return n[er].queries[e].queryList}function yA(n,e,t){let i=new ld((t&4)===4);return O0(n,e,i,i.destroy),(e[er]??=new yg).queries.push(new vg(i))-1}function _A(n,e,t){let i=ki();return i.firstCreatePass&&(EA(i,new _g(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),yA(i,Vt(),e)}function xA(n){return n.split(",").map(e=>e.trim())}function EA(n,e,t){n.queries===null&&(n.queries=new xg),n.queries.track(new Eg(e,t))}function jg(n,e){return n.queries.getByIndex(e)}function MA(n,e){let t=n[$e],i=jg(t,e);return i.crossesNgTemplate?Mg(t,n,e,[]):$E(t,n,i,e)}var ea=class{},Rd=class{};var md=class extends ea{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new hd(this);constructor(e,t,i,r=!0){super(),this.ngModuleType=e,this._parent=t;let o=pm(e);this._bootstrapComponents=uE(o.bootstrap),this._r3Injector=Gm(e,t,[{provide:ea,useValue:this},{provide:Gc,useValue:this.componentFactoryResolver},...i],Mc(e),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let e=this._r3Injector;!e.destroyed&&e.destroy(),this.destroyCbs.forEach(t=>t()),this.destroyCbs=null}onDestroy(e){this.destroyCbs.push(e)}},gd=class extends Rd{moduleType;constructor(e){super(),this.moduleType=e}create(e){return new md(this.moduleType,e,[])}};var Uc=class extends ea{injector;componentFactoryResolver=new hd(this);instance=null;constructor(e){super();let t=new Go([...e.providers,{provide:ea,useValue:this},{provide:Gc,useValue:this.componentFactoryResolver}],e.parent||Tc(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Wc(n,e,t=null){return new Uc({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var bA=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=Em(!1,t.type),r=i.length>0?Wc([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=je({token:n,providedIn:"environment",factory:()=>new n(it(rn))})}return n})();function ns(n){return xd(()=>{let e=qE(n),t=Ut(Ee({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Ig.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(bA).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||Hi.Emulated,styles:n.styles||Gr,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&ra("NgStandalone"),XE(t);let i=n.dependencies;return t.directiveDefs=Cx(i,SA),t.pipeDefs=Cx(i,p0),t.id=CA(t),t})}function SA(n){return jr(n)||mm(n)}function wA(n,e){if(n==null)return jo;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],o,s,a,c;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,c=r[3]||null):(o=r,s=r,a=wd.None,c=null),t[o]=[i,a,c],e[o]=s}return t}function TA(n){if(n==null)return jo;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function $g(n){return xd(()=>{let e=qE(n);return XE(e),e})}function qE(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||jo,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||Gr,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:wA(n.inputs,e),outputs:TA(n.outputs),debugInfo:null}}function XE(n){n.features?.forEach(e=>e(n))}function Cx(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let o=e(r);o!==null&&i.push(o)}return i}:null}function CA(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let o of i.join("|"))e=Math.imul(31,e)+o.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function IA(n,e,t,i,r,o,s,a){if(t.firstCreatePass){n.mergedAttrs=Ed(n.mergedAttrs,n.attrs);let u=n.tView=Ng(2,n,r,o,s,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),Xs(n,!1);let c=AA(t,e,n,i);Ju()&&Bg(t,e,c,n),Ks(c,e);let l=LE(c,e,c,n);e[i+Rn]=l,Lg(e,l),dA(l,n,e)}function YE(n,e,t,i,r,o,s,a,c,l,u){let d=t+Rn,f;if(e.firstCreatePass){if(f=Cd(e,d,4,s||null,a||null),l!=null){let h=Jr(e.consts,l);f.localNames=[];for(let m=0;m<h.length;m+=2)f.localNames.push(h[m],-1)}}else f=e.data[d];return IA(f,n,e,t,i,r,o,c),l!=null&&Vg(n,f,u),f}var AA=RA;function RA(n,e,t,i){return Qu(!0),e[mn].createComment("")}var qg=(()=>{class n{log(t){console.log(t)}warn(t){console.warn(t)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();var Xg=new Ve("");function jc(n){return!!n&&typeof n.then=="function"}function ZE(n){return!!n&&typeof n.subscribe=="function"}var KE=new Ve("");var Yg=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=ce(KE,{optional:!0})??[];injector=ce(Ji);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let o=An(this.injector,r);if(jc(o))t.push(o);else if(ZE(o)){let s=new Promise((a,c)=>{o.subscribe({complete:a,error:c})});t.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Dd=new Ve("");function JE(){Ap(()=>{let n="";throw new Oe(600,n)})}function QE(n){return n.isBoundToModule}var DA=10;var sa=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=ce(ir);afterRenderManager=ce(mE);zonelessEnabled=ce(Nc);rootEffectScheduler=ce(Ym);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new Mn;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=ce(to);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(qt(t=>!t))}constructor(){ce(ia,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=ce(rn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=Ji.NULL){return this._injector.get(ti).run(()=>{Pt(bt.BootstrapComponentStart);let s=t instanceof Id;if(!this._injector.get(Yg).done){let m="";throw new Oe(405,m)}let c;s?c=t:c=this._injector.get(Gc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=QE(c)?void 0:this._injector.get(ea),u=i||c.selector,d=c.create(r,[],u,l),f=d.location.nativeElement,h=d.injector.get(Xg,null);return h?.registerApplication(f),d.onDestroy(()=>{this.detachView(d.hostView),Lc(this.components,d),h?.unregisterApplication(f)}),this._loadComponent(d),Pt(bt.BootstrapComponentEnd,d),d})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Pt(bt.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Fg.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Pt(bt.ChangeDetectionEnd),new Oe(101,!1);let t=Ke(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,Ke(t),this.afterTick.next(),Pt(bt.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(es,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<DA;){Pt(bt.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Pt(bt.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Rc(r))continue;let o=i&&!this.zonelessEnabled?0:1;RE(r,o),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Rc(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;Lc(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(Dd,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>Lc(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Oe(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Lc(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function io(n,e,t,i,r,o,s,a){ra("NgControlFlow");let c=Vt(),l=ki(),u=Jr(l.consts,o);return YE(c,l,n,e,t,i,r,u,256,s,a),Zg}function Zg(n,e,t,i,r,o,s,a){ra("NgControlFlow");let c=Vt(),l=ki(),u=Jr(l.consts,o);return YE(c,l,n,e,t,i,r,u,512,s,a),Zg}function ro(n,e){ra("NgControlFlow");let t=Vt(),i=qu(),r=t[i]!==Tr?t[i]:-1,o=r!==-1?Ix(t,Rn+r):void 0,s=0;if(Ad(t,i,n)){let a=Ke(null);try{if(o!==void 0&&II(o,s),n!==-1){let c=Rn+n,l=Ix(t,c),u=NA(t[$e],c),d=FI(l,u,t),f=SE(t,u,e,{dehydratedView:d});OE(l,f,s,dg(u,d))}}finally{Ke(a)}}else if(o!==void 0){let a=CI(o,s);a!==void 0&&(a[wn]=e)}}function Ix(n,e){return n[e]}function NA(n,e){return Wu(n,e)}function Ax(n,e,t,i,r){bE(e,n,t,r?"class":"style",i)}function vd(n,e,t,i){let r=Vt(),o=r[$e],s=n+Rn,a=o.firstCreatePass?VE(s,r,2,e,lI,U0(),t,i):o.data[s];if(Kr(a)){let c=r[Qi].tracingService;if(c&&c.componentCreate){let l=o.data[a.directiveStart+a.componentOffset];return c.componentCreate(GE(l),()=>(Rx(n,e,r,a,i),vd))}}return Rx(n,e,r,a,i),vd}function Rx(n,e,t,i,r){if(EE(i,t,n,e,eM),Gu(i)){let o=t[$e];xE(o,t,i),oE(o,i,t)}r!=null&&Vg(t,i)}function Kg(){let n=ki(),e=Ui(),t=ME(e);return n.firstCreatePass&&zE(n,t),Lm(t)&&Om(),Pm(),t.classesWithoutHost!=null&&qT(t)&&Ax(n,t,Vt(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&XT(t)&&Ax(n,t,Vt(),t.stylesWithoutHost,!1),Kg}function aa(n,e,t,i){return vd(n,e,t,i),Kg(),aa}function ne(n,e,t,i){let r=Vt(),o=r[$e],s=n+Rn,a=o.firstCreatePass?qI(s,o,2,e,t,i):o.data[s];return EE(a,r,n,e,eM),i!=null&&Vg(r,a),ne}function se(){let n=Ui(),e=ME(n);return Lm(e)&&Om(),Pm(),se}function ii(n,e,t,i){return ne(n,e,t,i),se(),ii}var eM=(n,e,t,i,r)=>(Qu(!0),aE(e[mn],i,Q0()));function $c(){return Vt()}function qc(n,e,t){let i=Vt(),r=qu();if(Ad(i,r,e)){let o=ki(),s=J0();oI(s,i,n,e,i[mn],t)}return qc}var Xc="en-US";var PA=Xc;function tM(n){typeof n=="string"&&(PA=n.toLowerCase().replace(/_/g,"-"))}function qn(n,e,t){let i=Vt(),r=ki(),o=Ui();return(o.type&3||t)&&YI(o,r,i,t,i[mn],n,e,XI(o,i,e)),qn}function ca(n=1){return K0(n)}function Nd(n,e,t){return _A(n,e,t),Nd}function Pd(n){let e=Vt(),t=ki(),i=Hm();Yu(i+1);let r=jg(t,i);if(n.dirty&&D0(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let o=MA(e,i);n.reset(o,cC),n.notifyOnChanges()}return!0}return!1}function Ld(){return vA(Vt(),Hm())}function ed(n,e){return n<<17|e<<2}function ts(n){return n>>17&32767}function LA(n){return(n&2)==2}function OA(n,e){return n&131071|e<<17}function bg(n){return n|2}function ta(n){return(n&131068)>>2}function eg(n,e){return n&-131069|e<<2}function FA(n){return(n&1)===1}function Sg(n){return n|1}function kA(n,e,t,i,r,o){let s=o?e.classBindings:e.styleBindings,a=ts(s),c=ta(s);n[i]=t;let l=!1,u;if(Array.isArray(t)){let d=t;u=d[1],(u===null||Hs(d,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let f=ts(n[a+1]);n[i+1]=ed(f,a),f!==0&&(n[f+1]=eg(n[f+1],i)),n[a+1]=OA(n[a+1],i)}else n[i+1]=ed(a,0),a!==0&&(n[a+1]=eg(n[a+1],i)),a=i;else n[i+1]=ed(c,0),a===0?a=i:n[c+1]=eg(n[c+1],i),c=i;l&&(n[i+1]=bg(n[i+1])),Dx(n,u,i,!0),Dx(n,u,i,!1),UA(e,u,n,i,o),s=ed(a,c),o?e.classBindings=s:e.styleBindings=s}function UA(n,e,t,i,r){let o=r?n.residualClasses:n.residualStyles;o!=null&&typeof e=="string"&&Hs(o,e)>=0&&(t[i+1]=Sg(t[i+1]))}function Dx(n,e,t,i){let r=n[t+1],o=e===null,s=i?ts(r):ta(r),a=!1;for(;s!==0&&(a===!1||o);){let c=n[s],l=n[s+1];BA(c,e)&&(a=!0,n[s+1]=i?Sg(l):bg(l)),s=i?ts(l):ta(l)}a&&(n[t+1]=i?bg(r):Sg(r))}function BA(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?Hs(n,e)>=0:!1}function Od(n,e,t){return nM(n,e,t,!1),Od}function or(n,e){return nM(n,e,null,!0),or}function nM(n,e,t,i){let r=Vt(),o=ki(),s=G0(2);if(o.firstUpdatePass&&VA(o,n,s,i),e!==Tr&&Ad(r,s,e)){let a=o.data[Jo()];$A(o,a,r,r[mn],n,r[s+1]=qA(e,t),i,s)}}function HA(n,e){return e>=n.expandoStartIndex}function VA(n,e,t,i){let r=n.data;if(r[t+1]===null){let o=r[Jo()],s=HA(n,t);XA(o,i)&&e===null&&!s&&(e=!1),e=zA(r,o,e,i),kA(r,o,e,t,s,i)}}function zA(n,e,t,i){let r=q0(n),o=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=tg(null,n,e,t,i),t=Bc(t,e.attrs,i),o=null);else{let s=e.directiveStylingLast;if(s===-1||n[s]!==r)if(t=tg(r,n,e,t,i),o===null){let c=GA(n,e,i);c!==void 0&&Array.isArray(c)&&(c=tg(null,n,e,c[1],i),c=Bc(c,e.attrs,i),WA(n,e,i,c))}else o=jA(n,e,i)}return o!==void 0&&(i?e.residualClasses=o:e.residualStyles=o),t}function GA(n,e,t){let i=t?e.classBindings:e.styleBindings;if(ta(i)!==0)return n[ts(i)]}function WA(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[ts(r)]=i}function jA(n,e,t){let i,r=e.directiveEnd;for(let o=1+e.directiveStylingLast;o<r;o++){let s=n[o].hostAttrs;i=Bc(i,s,t)}return Bc(i,e.attrs,t)}function tg(n,e,t,i,r){let o=null,s=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<s&&(o=e[a],i=Bc(i,o.hostAttrs,r),o!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function Bc(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let o=0;o<e.length;o++){let s=e[o];typeof s=="number"?r=s:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),M0(n,s,t?!0:e[++o]))}return n===void 0?null:n}function $A(n,e,t,i,r,o,s,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=FA(l)?Nx(c,e,t,r,ta(l),s):void 0;if(!yd(u)){yd(o)||LA(l)&&(o=Nx(c,null,t,r,a,s));let d=Cm(Jo(),t);tI(i,s,d,r,o)}}function Nx(n,e,t,i,r,o){let s=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,d=u===null,f=t[r+1];f===Tr&&(f=d?Gr:void 0);let h=d?Hu(f,i):u===i?f:void 0;if(l&&!yd(h)&&(h=Hu(c,i)),yd(h)&&(a=h,s))return a;let m=n[r+1];r=s?ts(m):ta(m)}if(e!==null){let c=o?e.residualClasses:e.residualStyles;c!=null&&(a=Hu(c,i))}return a}function yd(n){return n!==void 0}function qA(n,e){return n==null||n===""||(typeof e=="string"?n=n+e:typeof n=="object"&&(n=Mc(sE(n)))),n}function XA(n,e){return(n.flags&(e?8:16))!==0}function ve(n,e=""){let t=Vt(),i=ki(),r=n+Rn,o=i.firstCreatePass?Cd(i,r,1,e,null):i.data[r],s=YA(i,t,o,e);t[r]=s,Ju()&&Bg(i,t,s,o),Xs(o,!1)}var YA=(n,e,t,i)=>(Qu(!0),yC(e[mn],i));function ZA(n,e,t,i=""){return Ad(n,qu(),t)?e+m0(t)+i:Tr}function oo(n){return Jg("",n),oo}function Jg(n,e,t){let i=Vt(),r=ZA(i,n,e,t);return r!==Tr&&KA(i,Jo(),r),Jg}function KA(n,e,t){let i=Cm(e,n);_C(n[mn],i,t)}var _d=class{ngModuleFactory;componentFactories;constructor(e,t){this.ngModuleFactory=e,this.componentFactories=t}},Qg=(()=>{class n{compileModuleSync(t){return new gd(t)}compileModuleAsync(t){return Promise.resolve(this.compileModuleSync(t))}compileModuleAndAllComponentsSync(t){let i=this.compileModuleSync(t),r=pm(t),o=uE(r.declarations).reduce((s,a)=>{let c=jr(a);return c&&s.push(new Qs(c)),s},[]);return new _d(i,o)}compileModuleAndAllComponentsAsync(t){return Promise.resolve(this.compileModuleAndAllComponentsSync(t))}clearCache(){}clearCacheFor(t){}getModuleId(t){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var iM=(()=>{class n{applicationErrorHandler=ce(ir);appRef=ce(sa);taskService=ce(to);ngZone=ce(ti);zonelessEnabled=ce(Nc);tracing=ce(ia,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new En;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(_c):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(ce(Xm,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?ix:Wm;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(_c+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function rM(){return[{provide:Bs,useExisting:iM},{provide:ti,useClass:xc},{provide:Nc,useValue:!0}]}function JA(){return typeof $localize<"u"&&$localize.locale||Xc}var ev=new Ve("",{factory:()=>ce(ev,{optional:!0,skipSelf:!0})||JA()});function so(n){return l0(n)}var lM=Symbol("InputSignalNode#UNSET"),cR=Ut(Ee({},cu),{transformFn:void 0,applyValueToInputSignal(n,e){Rs(n,e)}});function uM(n,e){let t=Object.create(cR);t.value=n,t.transformFn=e?.transform;function i(){if(cc(t),t.value===lM){let r=null;throw new Oe(-950,r)}return t.value}return i[Qn]=t,i}function oM(n,e){return uM(n,e)}function lR(n){return uM(lM,n)}var dM=(oM.required=lR,oM);var tv=new Ve(""),uR=new Ve("");function Yc(n){return!n.moduleRef}function dR(n){let e=Yc(n)?n.r3Injector:n.moduleRef.injector,t=e.get(ti);return t.run(()=>{Yc(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ir),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),Yc(n)){let o=()=>e.destroy(),s=n.platformInjector.get(tv);s.add(o),e.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>n.moduleRef.destroy(),s=n.platformInjector.get(tv);s.add(o),n.moduleRef.onDestroy(()=>{Lc(n.allPlatformModules,n.moduleRef),r.unsubscribe(),s.delete(o)})}return hR(i,t,()=>{let o=e.get(to),s=o.add(),a=e.get(Yg);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get(ev,Xc);if(tM(c||Xc),!e.get(uR,!0))return Yc(n)?e.get(sa):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(Yc(n)){let u=e.get(sa);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return fR?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{o.remove(s)})})})}var fR;function hR(n,e,t){try{let i=t();return jc(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var Fd=null;function pR(n=[],e){return Ji.create({name:e,providers:[{provide:wc,useValue:"platform"},{provide:tv,useValue:new Set([()=>Fd=null])},...n]})}function mR(n=[]){if(Fd)return Fd;let e=pR(n);return Fd=e,JE(),gR(e),e}function gR(n){let e=n.get(bd,null);An(n,()=>{e?.forEach(t=>t())})}var vR=1e4;var X$=vR-1e3;var fM=(()=>{class n{static __NG_ELEMENT_ID__=yR}return n})();function yR(n){return _R(Ui(),Vt(),(n&16)===16)}function _R(n,e,t){if(Kr(n)&&!t){let i=nr(n.index,e);return new no(i,i)}else if(n.type&175){let i=e[_i];return new no(i,e)}return null}function hM(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;Pt(bt.BootstrapApplicationStart);try{let o=r?.injector??mR(i),s=[rM(),ox,...t||[]],a=new Uc({providers:s,parent:o,debugName:"",runEnvironmentInitializers:!1});return dR({r3Injector:a.injector,platformInjector:o,rootComponent:e})}catch(o){return Promise.reject(o)}finally{Pt(bt.BootstrapApplicationEnd)}}var pM=null;function Cr(){return pM}function nv(n){pM??=n}var Zc=class{},kd=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>ce(mM),providedIn:"platform"})}return n})();var mM=(()=>{class n extends kd{_location;_history;_doc=ce(on);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Cr().getBaseHref(this._doc)}onPopState(t){let i=Cr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",t,!1),()=>i.removeEventListener("popstate",t)}onHashChange(t){let i=Cr().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",t,!1),()=>i.removeEventListener("hashchange",t)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(t){this._location.pathname=t}pushState(t,i,r){this._history.pushState(t,i,r)}replaceState(t,i,r){this._history.replaceState(t,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(t=0){this._history.go(t)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function yM(n,e){return n?e?n.endsWith("/")?e.startsWith("/")?n+e.slice(1):n+e:e.startsWith("/")?n+e:`${n}/${e}`:n:e}function gM(n){let e=n.search(/#|\?|$/);return n[e-1]==="/"?n.slice(0,e-1)+n.slice(e):n}function co(n){return n&&n[0]!=="?"?`?${n}`:n}var Ud=(()=>{class n{historyGo(t){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>ce(ER),providedIn:"root"})}return n})(),xR=new Ve(""),ER=(()=>{class n extends Ud{_platformLocation;_baseHref;_removeListenerFns=[];constructor(t,i){super(),this._platformLocation=t,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??ce(on).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(t){this._removeListenerFns.push(this._platformLocation.onPopState(t),this._platformLocation.onHashChange(t))}getBaseHref(){return this._baseHref}prepareExternalUrl(t){return yM(this._baseHref,t)}path(t=!1){let i=this._platformLocation.pathname+co(this._platformLocation.search),r=this._platformLocation.hash;return r&&t?`${i}${r}`:i}pushState(t,i,r,o){let s=this.prepareExternalUrl(r+co(o));this._platformLocation.pushState(t,i,s)}replaceState(t,i,r,o){let s=this.prepareExternalUrl(r+co(o));this._platformLocation.replaceState(t,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(t=0){this._platformLocation.historyGo?.(t)}static \u0275fac=function(i){return new(i||n)(it(kd),it(xR,8))};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var la=(()=>{class n{_subject=new Mn;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(t){this._locationStrategy=t;let i=this._locationStrategy.getBaseHref();this._basePath=SR(gM(vM(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(t=!1){return this.normalize(this._locationStrategy.path(t))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(t,i=""){return this.path()==this.normalize(t+co(i))}normalize(t){return n.stripTrailingSlash(bR(this._basePath,vM(t)))}prepareExternalUrl(t){return t&&t[0]!=="/"&&(t="/"+t),this._locationStrategy.prepareExternalUrl(t)}go(t,i="",r=null){this._locationStrategy.pushState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+co(i)),r)}replaceState(t,i="",r=null){this._locationStrategy.replaceState(r,"",t,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(t+co(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(t=0){this._locationStrategy.historyGo?.(t)}onUrlChange(t){return this._urlChangeListeners.push(t),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(t);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(t="",i){this._urlChangeListeners.forEach(r=>r(t,i))}subscribe(t,i,r){return this._subject.subscribe({next:t,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=co;static joinWithSlash=yM;static stripTrailingSlash=gM;static \u0275fac=function(i){return new(i||n)(it(Ud))};static \u0275prov=je({token:n,factory:()=>MR(),providedIn:"root"})}return n})();function MR(){return new la(it(Ud))}function bR(n,e){if(!n||!e.startsWith(n))return e;let t=e.substring(n.length);return t===""||["/",";","?","#"].includes(t[0])?t:e}function vM(n){return n.replace(/\/index.html$/,"")}function SR(n){if(new RegExp("^(https?:)?//").test(n)){let[,t]=n.split(/\/\/[^\/]+/);return t}return n}function iv(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,o]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(o)}return null}var Kc=class{};var _M="browser";var Jc=class{_doc;constructor(e){this._doc=e}manager},Bd=(()=>{class n extends Jc{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,o){return t.addEventListener(i,r,o),()=>this.removeEventListener(t,i,r,o)}removeEventListener(t,i,r,o){return t.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||n)(it(on))};static \u0275prov=je({token:n,factory:n.\u0275fac})}return n})(),zd=new Ve(""),av=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(s=>{s.manager=this});let r=t.filter(s=>!(s instanceof Bd));this._plugins=r.slice().reverse();let o=t.find(s=>s instanceof Bd);o&&this._plugins.push(o)}addEventListener(t,i,r,o){return this._findPluginFor(i).addEventListener(t,i,r,o)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(o=>o.supports(t)),!i)throw new Oe(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(it(zd),it(ti))};static \u0275prov=je({token:n,factory:n.\u0275fac})}return n})(),rv="ng-app-id";function xM(n){for(let e of n)e.remove()}function EM(n,e){let t=e.createElement("style");return t.textContent=n,t}function wR(n,e,t,i){let r=n.head?.querySelectorAll(`style[${rv}="${e}"],link[${rv}="${e}"]`);if(r)for(let o of r)o.removeAttribute(rv),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&t.set(o.textContent,{usage:0,elements:[o]})}function sv(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var cv=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,o={}){this.doc=t,this.appId=i,this.nonce=r,wR(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,EM);i?.forEach(r=>this.addUsage(r,this.external,sv))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let o=i.get(t);o?o.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(xM(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])xM(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,EM(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,sv(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(it(on),it(Md),it(Sd,8),it(zc))};static \u0275prov=je({token:n,factory:n.\u0275fac})}return n})(),ov={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},lv=/%COMP%/g;var bM="%COMP%",TR=`_nghost-${bM}`,CR=`_ngcontent-${bM}`,IR=!0,AR=new Ve("",{factory:()=>IR});function RR(n){return CR.replace(lv,n)}function DR(n){return TR.replace(lv,n)}function SM(n,e){return e.map(t=>t.replace(lv,n))}var uv=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,o,s,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new Qc(t,s,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Vd?r.applyToHost(t):r instanceof el&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,d=this.tracingService;switch(i.encapsulation){case Hi.Emulated:o=new Vd(c,l,i,this.appId,u,s,a,d);break;case Hi.ShadowDom:return new Hd(c,t,i,s,a,this.nonce,d,l);case Hi.ExperimentalIsolatedShadowDom:return new Hd(c,t,i,s,a,this.nonce,d);default:o=new el(c,l,i,u,s,a,d);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(it(av),it(cv),it(Md),it(AR),it(on),it(ti),it(Sd),it(ia,8))};static \u0275prov=je({token:n,factory:n.\u0275fac})}return n})(),Qc=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(ov[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(MM(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(MM(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Oe(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let o=ov[r];o?e.setAttributeNS(o,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=ov[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(wr.DashCase|wr.Important)?e.style.setProperty(t,i,r&wr.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&wr.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=Cr().getGlobalEventTarget(this.doc,e),!e))throw new Oe(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(e,t,o)),this.eventManager.addEventListener(e,t,o,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function MM(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Hd=class extends Qc{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,o,s,a,c){super(e,r,o,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=SM(i.id,l);for(let d of l){let f=document.createElement("style");s&&f.setAttribute("nonce",s),f.textContent=d,this.shadowRoot.appendChild(f)}let u=i.getExternalStyles?.();if(u)for(let d of u){let f=sv(d,r);s&&f.setAttribute("nonce",s),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},el=class extends Qc{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,o,s,a,c){super(e,o,s,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?SM(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Js.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Vd=class extends el{contentAttr;hostAttr;constructor(e,t,i,r,o,s,a,c){let l=r+"-"+i.id;super(e,t,i,o,s,a,c,l),this.contentAttr=RR(l),this.hostAttr=DR(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var Gd=class n extends Zc{supportsDOMEvents=!0;static makeCurrent(){nv(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=NR();return t==null?null:PR(t)}resetBaseElement(){tl=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return iv(document.cookie,e)}},tl=null;function NR(){return tl=tl||document.head.querySelector("base"),tl?tl.getAttribute("href"):null}function PR(n){return new URL(n,document.baseURI).pathname}var LR=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac})}return n})(),wM=["alt","control","meta","shift"],OR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},FR={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},TM=(()=>{class n extends Jc{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,o){let s=n.parseEventName(i),a=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Cr().onAndCancel(t,s.domEventName,a,o))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=n._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),wM.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),s+=l+".")}),s+=o,i.length!=0||o.length===0)return null;let c={};return c.domEventName=r,c.fullKey=s,c}static matchEventFullKeyCode(t,i){let r=OR[t.key]||t.key,o="";return i.indexOf("code.")>-1&&(r=t.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),wM.forEach(s=>{if(s!==r){let a=FR[s];a(t)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(t,i,r){return o=>{n.matchEventFullKeyCode(o,t)&&r.runGuarded(()=>i(o))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(it(on))};static \u0275prov=je({token:n,factory:n.\u0275fac})}return n})();async function dv(n,e,t){let i=Ee({rootComponent:n},kR(e,t));return hM(i)}function kR(n,e){return{platformRef:e?.platformRef,appProviders:[...zR,...n?.providers??[]],platformProviders:VR}}function UR(){Gd.makeCurrent()}function BR(){return new Sr}function HR(){return Ag(document),document}var VR=[{provide:zc,useValue:_M},{provide:bd,useValue:UR,multi:!0},{provide:on,useFactory:HR}];var zR=[{provide:wc,useValue:"root"},{provide:Sr,useFactory:BR},{provide:zd,useClass:Bd,multi:!0},{provide:zd,useClass:TM,multi:!0},uv,cv,av,{provide:es,useExisting:uv},{provide:Kc,useClass:LR},[]];var CM=(()=>{class n{_doc;constructor(t){this._doc=t}getTitle(){return this._doc.title}setTitle(t){this._doc.title=t||""}static \u0275fac=function(i){return new(i||n)(it(on))};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Ze="primary",hl=Symbol("RouteTitle"),gv=class{params;constructor(e){this.params=e||{}}has(e){return Object.prototype.hasOwnProperty.call(this.params,e)}get(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t[0]:t}return null}getAll(e){if(this.has(e)){let t=this.params[e];return Array.isArray(t)?t:[t]}return[]}get keys(){return Object.keys(this.params)}};function ha(n){return new gv(n)}function fv(n,e,t){for(let i=0;i<n.length;i++){let r=n[i],o=e[i];if(r[0]===":")t[r.substring(1)]=o;else if(r!==o.path)return!1}return!0}function jR(n,e,t){let i=t.path.split("/"),r=i.indexOf("**");if(r===-1){if(i.length>n.length||t.pathMatch==="full"&&(e.hasChildren()||i.length<n.length))return null;let c={},l=n.slice(0,i.length);return fv(i,l,c)?{consumed:l,posParams:c}:null}if(r!==i.lastIndexOf("**"))return null;let o=i.slice(0,r),s=i.slice(r+1);if(o.length+s.length>n.length||t.pathMatch==="full"&&e.hasChildren()&&t.path!=="**")return null;let a={};return!fv(o,n.slice(0,o.length),a)||!fv(s,n.slice(n.length-s.length),a)?null:{consumed:n,posParams:a}}function Yd(n){return new Promise((e,t)=>{n.pipe(Mr()).subscribe({next:i=>e(i),error:i=>t(i)})})}function $R(n,e){if(n.length!==e.length)return!1;for(let t=0;t<n.length;++t)if(!sr(n[t],e[t]))return!1;return!0}function sr(n,e){let t=n?vv(n):void 0,i=e?vv(e):void 0;if(!t||!i||t.length!=i.length)return!1;let r;for(let o=0;o<t.length;o++)if(r=t[o],!OM(n[r],e[r]))return!1;return!0}function vv(n){return[...Object.keys(n),...Object.getOwnPropertySymbols(n)]}function OM(n,e){if(Array.isArray(n)&&Array.isArray(e)){if(n.length!==e.length)return!1;let t=[...n].sort(),i=[...e].sort();return t.every((r,o)=>i[o]===r)}else return n===e}function qR(n){return n.length>0?n[n.length-1]:null}function as(n){return Tu(n)?n:jc(n)?tn(Promise.resolve(n)):dt(n)}function FM(n){return Tu(n)?Yd(n):Promise.resolve(n)}var XR={exact:BM,subset:HM},kM={exact:YR,subset:ZR,ignored:()=>!0},UM={paths:"exact",fragment:"ignored",matrixParams:"ignored",queryParams:"exact"},yv={paths:"subset",fragment:"ignored",matrixParams:"ignored",queryParams:"subset"};function IM(n,e,t){return XR[t.paths](n.root,e.root,t.matrixParams)&&kM[t.queryParams](n.queryParams,e.queryParams)&&!(t.fragment==="exact"&&n.fragment!==e.fragment)}function YR(n,e){return sr(n,e)}function BM(n,e,t){if(!rs(n.segments,e.segments)||!$d(n.segments,e.segments,t)||n.numberOfChildren!==e.numberOfChildren)return!1;for(let i in e.children)if(!n.children[i]||!BM(n.children[i],e.children[i],t))return!1;return!0}function ZR(n,e){return Object.keys(e).length<=Object.keys(n).length&&Object.keys(e).every(t=>OM(n[t],e[t]))}function HM(n,e,t){return VM(n,e,e.segments,t)}function VM(n,e,t,i){if(n.segments.length>t.length){let r=n.segments.slice(0,t.length);return!(!rs(r,t)||e.hasChildren()||!$d(r,t,i))}else if(n.segments.length===t.length){if(!rs(n.segments,t)||!$d(n.segments,t,i))return!1;for(let r in e.children)if(!n.children[r]||!HM(n.children[r],e.children[r],i))return!1;return!0}else{let r=t.slice(0,n.segments.length),o=t.slice(n.segments.length);return!rs(n.segments,r)||!$d(n.segments,r,i)||!n.children[Ze]?!1:VM(n.children[Ze],e,o,i)}}function $d(n,e,t){return e.every((i,r)=>kM[t](n[r].parameters,i.parameters))}var zi=class{root;queryParams;fragment;_queryParamMap;constructor(e=new It([],{}),t={},i=null){this.root=e,this.queryParams=t,this.fragment=i}get queryParamMap(){return this._queryParamMap??=ha(this.queryParams),this._queryParamMap}toString(){return QR.serialize(this)}},It=class{segments;children;parent=null;constructor(e,t){this.segments=e,this.children=t,Object.values(t).forEach(i=>i.parent=this)}hasChildren(){return this.numberOfChildren>0}get numberOfChildren(){return Object.keys(this.children).length}toString(){return qd(this)}},is=class{path;parameters;_parameterMap;constructor(e,t){this.path=e,this.parameters=t}get parameterMap(){return this._parameterMap??=ha(this.parameters),this._parameterMap}toString(){return GM(this)}};function KR(n,e){return rs(n,e)&&n.every((t,i)=>sr(t.parameters,e[i].parameters))}function rs(n,e){return n.length!==e.length?!1:n.every((t,i)=>t.path===e[i].path)}function JR(n,e){let t=[];return Object.entries(n.children).forEach(([i,r])=>{i===Ze&&(t=t.concat(e(r,i)))}),Object.entries(n.children).forEach(([i,r])=>{i!==Ze&&(t=t.concat(e(r,i)))}),t}var sf=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>new os,providedIn:"root"})}return n})(),os=class{parse(e){let t=new xv(e);return new zi(t.parseRootSegment(),t.parseQueryParams(),t.parseFragment())}serialize(e){let t=`/${nl(e.root,!0)}`,i=nD(e.queryParams),r=typeof e.fragment=="string"?`#${eD(e.fragment)}`:"";return`${t}${i}${r}`}},QR=new os;function qd(n){return n.segments.map(e=>GM(e)).join("/")}function nl(n,e){if(!n.hasChildren())return qd(n);if(e){let t=n.children[Ze]?nl(n.children[Ze],!1):"",i=[];return Object.entries(n.children).forEach(([r,o])=>{r!==Ze&&i.push(`${r}:${nl(o,!1)}`)}),i.length>0?`${t}(${i.join("//")})`:t}else{let t=JR(n,(i,r)=>r===Ze?[nl(n.children[Ze],!1)]:[`${r}:${nl(i,!1)}`]);return Object.keys(n.children).length===1&&n.children[Ze]!=null?`${qd(n)}/${t[0]}`:`${qd(n)}/(${t.join("//")})`}}function zM(n){return encodeURIComponent(n).replace(/%40/g,"@").replace(/%3A/gi,":").replace(/%24/g,"$").replace(/%2C/gi,",")}function Wd(n){return zM(n).replace(/%3B/gi,";")}function eD(n){return encodeURI(n)}function _v(n){return zM(n).replace(/\(/g,"%28").replace(/\)/g,"%29").replace(/%26/gi,"&")}function Xd(n){return decodeURIComponent(n)}function AM(n){return Xd(n.replace(/\+/g,"%20"))}function GM(n){return`${_v(n.path)}${tD(n.parameters)}`}function tD(n){return Object.entries(n).map(([e,t])=>`;${_v(e)}=${_v(t)}`).join("")}function nD(n){let e=Object.entries(n).map(([t,i])=>Array.isArray(i)?i.map(r=>`${Wd(t)}=${Wd(r)}`).join("&"):`${Wd(t)}=${Wd(i)}`).filter(t=>t);return e.length?`?${e.join("&")}`:""}var iD=/^[^\/()?;#]+/;function hv(n){let e=n.match(iD);return e?e[0]:""}var rD=/^[^\/()?;=#]+/;function oD(n){let e=n.match(rD);return e?e[0]:""}var sD=/^[^=?&#]+/;function aD(n){let e=n.match(sD);return e?e[0]:""}var cD=/^[^&#]+/;function lD(n){let e=n.match(cD);return e?e[0]:""}var xv=class{url;remaining;constructor(e){this.url=e,this.remaining=e}parseRootSegment(){return this.consumeOptional("/"),this.remaining===""||this.peekStartsWith("?")||this.peekStartsWith("#")?new It([],{}):new It([],this.parseChildren())}parseQueryParams(){let e={};if(this.consumeOptional("?"))do this.parseQueryParam(e);while(this.consumeOptional("&"));return e}parseFragment(){return this.consumeOptional("#")?decodeURIComponent(this.remaining):null}parseChildren(e=0){if(e>50)throw new Oe(4010,!1);if(this.remaining==="")return{};this.consumeOptional("/");let t=[];for(this.peekStartsWith("(")||t.push(this.parseSegment());this.peekStartsWith("/")&&!this.peekStartsWith("//")&&!this.peekStartsWith("/(");)this.capture("/"),t.push(this.parseSegment());let i={};this.peekStartsWith("/(")&&(this.capture("/"),i=this.parseParens(!0,e));let r={};return this.peekStartsWith("(")&&(r=this.parseParens(!1,e)),(t.length>0||Object.keys(i).length>0)&&(r[Ze]=new It(t,i)),r}parseSegment(){let e=hv(this.remaining);if(e===""&&this.peekStartsWith(";"))throw new Oe(4009,!1);return this.capture(e),new is(Xd(e),this.parseMatrixParams())}parseMatrixParams(){let e={};for(;this.consumeOptional(";");)this.parseParam(e);return e}parseParam(e){let t=oD(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let r=hv(this.remaining);r&&(i=r,this.capture(i))}e[Xd(t)]=Xd(i)}parseQueryParam(e){let t=aD(this.remaining);if(!t)return;this.capture(t);let i="";if(this.consumeOptional("=")){let s=lD(this.remaining);s&&(i=s,this.capture(i))}let r=AM(t),o=AM(i);if(e.hasOwnProperty(r)){let s=e[r];Array.isArray(s)||(s=[s],e[r]=s),s.push(o)}else e[r]=o}parseParens(e,t){let i={};for(this.capture("(");!this.consumeOptional(")")&&this.remaining.length>0;){let r=hv(this.remaining),o=this.remaining[r.length];if(o!=="/"&&o!==")"&&o!==";")throw new Oe(4010,!1);let s;r.indexOf(":")>-1?(s=r.slice(0,r.indexOf(":")),this.capture(s),this.capture(":")):e&&(s=Ze);let a=this.parseChildren(t+1);i[s??Ze]=Object.keys(a).length===1&&a[Ze]?a[Ze]:new It([],a),this.consumeOptional("//")}return i}peekStartsWith(e){return this.remaining.startsWith(e)}consumeOptional(e){return this.peekStartsWith(e)?(this.remaining=this.remaining.substring(e.length),!0):!1}capture(e){if(!this.consumeOptional(e))throw new Oe(4011,!1)}};function WM(n){return n.segments.length>0?new It([],{[Ze]:n}):n}function jM(n){let e={};for(let[i,r]of Object.entries(n.children)){let o=jM(r);if(i===Ze&&o.segments.length===0&&o.hasChildren())for(let[s,a]of Object.entries(o.children))e[s]=a;else(o.segments.length>0||o.hasChildren())&&(e[i]=o)}let t=new It(n.segments,e);return uD(t)}function uD(n){if(n.numberOfChildren===1&&n.children[Ze]){let e=n.children[Ze];return new It(n.segments.concat(e.segments),e.children)}return n}function pa(n){return n instanceof zi}function dD(n,e,t=null,i=null,r=new os){let o=$M(n);return qM(o,e,t,i,r)}function $M(n){let e;function t(o){let s={};for(let c of o.children){let l=t(c);s[c.outlet]=l}let a=new It(o.url,s);return o===n&&(e=a),a}let i=t(n.root),r=WM(i);return e??r}function qM(n,e,t,i,r){let o=n;for(;o.parent;)o=o.parent;if(e.length===0)return pv(o,o,o,t,i,r);let s=fD(e);if(s.toRoot())return pv(o,o,new It([],{}),t,i,r);let a=hD(s,o,n),c=a.processChildren?rl(a.segmentGroup,a.index,s.commands):YM(a.segmentGroup,a.index,s.commands);return pv(o,a.segmentGroup,c,t,i,r)}function Zd(n){return typeof n=="object"&&n!=null&&!n.outlets&&!n.segmentPath}function al(n){return typeof n=="object"&&n!=null&&n.outlets}function RM(n,e,t){n||="\u0275";let i=new zi;return i.queryParams={[n]:e},t.parse(t.serialize(i)).queryParams[n]}function pv(n,e,t,i,r,o){let s={};for(let[l,u]of Object.entries(i??{}))s[l]=Array.isArray(u)?u.map(d=>RM(l,d,o)):RM(l,u,o);let a;n===e?a=t:a=XM(n,e,t);let c=WM(jM(a));return new zi(c,s,r)}function XM(n,e,t){let i={};return Object.entries(n.children).forEach(([r,o])=>{o===e?i[r]=t:i[r]=XM(o,e,t)}),new It(n.segments,i)}var Kd=class{isAbsolute;numberOfDoubleDots;commands;constructor(e,t,i){if(this.isAbsolute=e,this.numberOfDoubleDots=t,this.commands=i,e&&i.length>0&&Zd(i[0]))throw new Oe(4003,!1);let r=i.find(al);if(r&&r!==qR(i))throw new Oe(4004,!1)}toRoot(){return this.isAbsolute&&this.commands.length===1&&this.commands[0]=="/"}};function fD(n){if(typeof n[0]=="string"&&n.length===1&&n[0]==="/")return new Kd(!0,0,n);let e=0,t=!1,i=n.reduce((r,o,s)=>{if(typeof o=="object"&&o!=null){if(o.outlets){let a={};return Object.entries(o.outlets).forEach(([c,l])=>{a[c]=typeof l=="string"?l.split("/"):l}),[...r,{outlets:a}]}if(o.segmentPath)return[...r,o.segmentPath]}return typeof o!="string"?[...r,o]:s===0?(o.split("/").forEach((a,c)=>{c==0&&a==="."||(c==0&&a===""?t=!0:a===".."?e++:a!=""&&r.push(a))}),r):[...r,o]},[]);return new Kd(t,e,i)}var da=class{segmentGroup;processChildren;index;constructor(e,t,i){this.segmentGroup=e,this.processChildren=t,this.index=i}};function hD(n,e,t){if(n.isAbsolute)return new da(e,!0,0);if(!t)return new da(e,!1,NaN);if(t.parent===null)return new da(t,!0,0);let i=Zd(n.commands[0])?0:1,r=t.segments.length-1+i;return pD(t,r,n.numberOfDoubleDots)}function pD(n,e,t){let i=n,r=e,o=t;for(;o>r;){if(o-=r,i=i.parent,!i)throw new Oe(4005,!1);r=i.segments.length}return new da(i,!1,r-o)}function mD(n){return al(n[0])?n[0].outlets:{[Ze]:n}}function YM(n,e,t){if(n??=new It([],{}),n.segments.length===0&&n.hasChildren())return rl(n,e,t);let i=gD(n,e,t),r=t.slice(i.commandIndex);if(i.match&&i.pathIndex<n.segments.length){let o=new It(n.segments.slice(0,i.pathIndex),{});return o.children[Ze]=new It(n.segments.slice(i.pathIndex),n.children),rl(o,0,r)}else return i.match&&r.length===0?new It(n.segments,{}):i.match&&!n.hasChildren()?Ev(n,e,t):i.match?rl(n,0,r):Ev(n,e,t)}function rl(n,e,t){if(t.length===0)return new It(n.segments,{});{let i=mD(t),r={};if(Object.keys(i).some(o=>o!==Ze)&&n.children[Ze]&&n.numberOfChildren===1&&n.children[Ze].segments.length===0){let o=rl(n.children[Ze],e,t);return new It(n.segments,o.children)}return Object.entries(i).forEach(([o,s])=>{typeof s=="string"&&(s=[s]),s!==null&&(r[o]=YM(n.children[o],e,s))}),Object.entries(n.children).forEach(([o,s])=>{i[o]===void 0&&(r[o]=s)}),new It(n.segments,r)}}function gD(n,e,t){let i=0,r=e,o={match:!1,pathIndex:0,commandIndex:0};for(;r<n.segments.length;){if(i>=t.length)return o;let s=n.segments[r],a=t[i];if(al(a))break;let c=`${a}`,l=i<t.length-1?t[i+1]:null;if(r>0&&c===void 0)break;if(c&&l&&typeof l=="object"&&l.outlets===void 0){if(!NM(c,l,s))return o;i+=2}else{if(!NM(c,{},s))return o;i++}r++}return{match:!0,pathIndex:r,commandIndex:i}}function Ev(n,e,t){let i=n.segments.slice(0,e),r=0;for(;r<t.length;){let o=t[r];if(al(o)){let c=vD(o.outlets);return new It(i,c)}if(r===0&&Zd(t[0])){let c=n.segments[e];i.push(new is(c.path,DM(t[0]))),r++;continue}let s=al(o)?o.outlets[Ze]:`${o}`,a=r<t.length-1?t[r+1]:null;s&&a&&Zd(a)?(i.push(new is(s,DM(a))),r+=2):(i.push(new is(s,{})),r++)}return new It(i,{})}function vD(n){let e={};return Object.entries(n).forEach(([t,i])=>{typeof i=="string"&&(i=[i]),i!==null&&(e[t]=Ev(new It([],{}),0,i))}),e}function DM(n){let e={};return Object.entries(n).forEach(([t,i])=>e[t]=`${i}`),e}function NM(n,e,t){return n==t.path&&sr(e,t.parameters)}var ol="imperative",Tn=(function(n){return n[n.NavigationStart=0]="NavigationStart",n[n.NavigationEnd=1]="NavigationEnd",n[n.NavigationCancel=2]="NavigationCancel",n[n.NavigationError=3]="NavigationError",n[n.RoutesRecognized=4]="RoutesRecognized",n[n.ResolveStart=5]="ResolveStart",n[n.ResolveEnd=6]="ResolveEnd",n[n.GuardsCheckStart=7]="GuardsCheckStart",n[n.GuardsCheckEnd=8]="GuardsCheckEnd",n[n.RouteConfigLoadStart=9]="RouteConfigLoadStart",n[n.RouteConfigLoadEnd=10]="RouteConfigLoadEnd",n[n.ChildActivationStart=11]="ChildActivationStart",n[n.ChildActivationEnd=12]="ChildActivationEnd",n[n.ActivationStart=13]="ActivationStart",n[n.ActivationEnd=14]="ActivationEnd",n[n.Scroll=15]="Scroll",n[n.NavigationSkipped=16]="NavigationSkipped",n})(Tn||{}),bi=class{id;url;constructor(e,t){this.id=e,this.url=t}},ma=class extends bi{type=Tn.NavigationStart;navigationTrigger;restoredState;constructor(e,t,i="imperative",r=null){super(e,t),this.navigationTrigger=i,this.restoredState=r}toString(){return`NavigationStart(id: ${this.id}, url: '${this.url}')`}},lo=class extends bi{urlAfterRedirects;type=Tn.NavigationEnd;constructor(e,t,i){super(e,t),this.urlAfterRedirects=i}toString(){return`NavigationEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}')`}},Hn=(function(n){return n[n.Redirect=0]="Redirect",n[n.SupersededByNewNavigation=1]="SupersededByNewNavigation",n[n.NoDataFromResolver=2]="NoDataFromResolver",n[n.GuardRejected=3]="GuardRejected",n[n.Aborted=4]="Aborted",n})(Hn||{}),Jd=(function(n){return n[n.IgnoredSameUrlNavigation=0]="IgnoredSameUrlNavigation",n[n.IgnoredByUrlHandlingStrategy=1]="IgnoredByUrlHandlingStrategy",n})(Jd||{}),Vi=class extends bi{reason;code;type=Tn.NavigationCancel;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}toString(){return`NavigationCancel(id: ${this.id}, url: '${this.url}')`}};function ZM(n){return n instanceof Vi&&(n.code===Hn.Redirect||n.code===Hn.SupersededByNewNavigation)}var uo=class extends bi{reason;code;type=Tn.NavigationSkipped;constructor(e,t,i,r){super(e,t),this.reason=i,this.code=r}},ga=class extends bi{error;target;type=Tn.NavigationError;constructor(e,t,i,r){super(e,t),this.error=i,this.target=r}toString(){return`NavigationError(id: ${this.id}, url: '${this.url}', error: ${this.error})`}},Qd=class extends bi{urlAfterRedirects;state;type=Tn.RoutesRecognized;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`RoutesRecognized(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Mv=class extends bi{urlAfterRedirects;state;type=Tn.GuardsCheckStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`GuardsCheckStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},bv=class extends bi{urlAfterRedirects;state;shouldActivate;type=Tn.GuardsCheckEnd;constructor(e,t,i,r,o){super(e,t),this.urlAfterRedirects=i,this.state=r,this.shouldActivate=o}toString(){return`GuardsCheckEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state}, shouldActivate: ${this.shouldActivate})`}},Sv=class extends bi{urlAfterRedirects;state;type=Tn.ResolveStart;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveStart(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},wv=class extends bi{urlAfterRedirects;state;type=Tn.ResolveEnd;constructor(e,t,i,r){super(e,t),this.urlAfterRedirects=i,this.state=r}toString(){return`ResolveEnd(id: ${this.id}, url: '${this.url}', urlAfterRedirects: '${this.urlAfterRedirects}', state: ${this.state})`}},Tv=class{route;type=Tn.RouteConfigLoadStart;constructor(e){this.route=e}toString(){return`RouteConfigLoadStart(path: ${this.route.path})`}},Cv=class{route;type=Tn.RouteConfigLoadEnd;constructor(e){this.route=e}toString(){return`RouteConfigLoadEnd(path: ${this.route.path})`}},Iv=class{snapshot;type=Tn.ChildActivationStart;constructor(e){this.snapshot=e}toString(){return`ChildActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Av=class{snapshot;type=Tn.ChildActivationEnd;constructor(e){this.snapshot=e}toString(){return`ChildActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Rv=class{snapshot;type=Tn.ActivationStart;constructor(e){this.snapshot=e}toString(){return`ActivationStart(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}},Dv=class{snapshot;type=Tn.ActivationEnd;constructor(e){this.snapshot=e}toString(){return`ActivationEnd(path: '${this.snapshot.routeConfig&&this.snapshot.routeConfig.path||""}')`}};var va=class{},cl=class{},ya=class{url;navigationBehaviorOptions;constructor(e,t){this.url=e,this.navigationBehaviorOptions=t}};function yD(n){return!(n instanceof va)&&!(n instanceof ya)&&!(n instanceof cl)}var Nv=class{rootInjector;outlet=null;route=null;children;attachRef=null;get injector(){return this.route?.snapshot._environmentInjector??this.rootInjector}constructor(e){this.rootInjector=e,this.children=new pl(this.rootInjector)}},pl=(()=>{class n{rootInjector;contexts=new Map;constructor(t){this.rootInjector=t}onChildOutletCreated(t,i){let r=this.getOrCreateContext(t);r.outlet=i,this.contexts.set(t,r)}onChildOutletDestroyed(t){let i=this.getContext(t);i&&(i.outlet=null,i.attachRef=null)}onOutletDeactivated(){let t=this.contexts;return this.contexts=new Map,t}onOutletReAttached(t){this.contexts=t}getOrCreateContext(t){let i=this.getContext(t);return i||(i=new Nv(this.rootInjector),this.contexts.set(t,i)),i}getContext(t){return this.contexts.get(t)||null}static \u0275fac=function(i){return new(i||n)(it(rn))};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),ef=class{_root;constructor(e){this._root=e}get root(){return this._root.value}parent(e){let t=this.pathFromRoot(e);return t.length>1?t[t.length-2]:null}children(e){let t=Pv(e,this._root);return t?t.children.map(i=>i.value):[]}firstChild(e){let t=Pv(e,this._root);return t&&t.children.length>0?t.children[0].value:null}siblings(e){let t=Lv(e,this._root);return t.length<2?[]:t[t.length-2].children.map(r=>r.value).filter(r=>r!==e)}pathFromRoot(e){return Lv(e,this._root).map(t=>t.value)}};function Pv(n,e){if(n===e.value)return e;for(let t of e.children){let i=Pv(n,t);if(i)return i}return null}function Lv(n,e){if(n===e.value)return[e];for(let t of e.children){let i=Lv(n,t);if(i.length)return i.unshift(e),i}return[]}var ri=class{value;children;constructor(e,t){this.value=e,this.children=t}toString(){return`TreeNode(${this.value})`}};function ua(n){let e={};return n&&n.children.forEach(t=>e[t.value.outlet]=t),e}var tf=class extends ef{snapshot;constructor(e,t){super(e),this.snapshot=t,Wv(this,e)}toString(){return this.snapshot.toString()}};function KM(n,e){let t=_D(n,e),i=new bn([new is("",{})]),r=new bn({}),o=new bn({}),s=new bn({}),a=new bn(""),c=new ss(i,r,s,a,o,Ze,n,t.root);return c.snapshot=t.root,new tf(new ri(c,[]),t)}function _D(n,e){let t={},i={},r={},s=new ll([],t,r,"",i,Ze,n,null,{},e);return new nf("",new ri(s,[]))}var ss=class{urlSubject;paramsSubject;queryParamsSubject;fragmentSubject;dataSubject;outlet;component;snapshot;_futureSnapshot;_routerState;_paramMap;_queryParamMap;title;url;params;queryParams;fragment;data;constructor(e,t,i,r,o,s,a,c){this.urlSubject=e,this.paramsSubject=t,this.queryParamsSubject=i,this.fragmentSubject=r,this.dataSubject=o,this.outlet=s,this.component=a,this._futureSnapshot=c,this.title=this.dataSubject?.pipe(qt(l=>l[hl]))??dt(void 0),this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=o}get routeConfig(){return this._futureSnapshot.routeConfig}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=this.params.pipe(qt(e=>ha(e))),this._paramMap}get queryParamMap(){return this._queryParamMap??=this.queryParams.pipe(qt(e=>ha(e))),this._queryParamMap}toString(){return this.snapshot?this.snapshot.toString():`Future(${this._futureSnapshot})`}};function Gv(n,e,t="emptyOnly"){let i,{routeConfig:r}=n;return e!==null&&(t==="always"||r?.path===""||!e.component&&!e.routeConfig?.loadComponent)?i={params:Ee(Ee({},e.params),n.params),data:Ee(Ee({},e.data),n.data),resolve:Ee(Ee(Ee(Ee({},n.data),e.data),r?.data),n._resolvedData)}:i={params:Ee({},n.params),data:Ee({},n.data),resolve:Ee(Ee({},n.data),n._resolvedData??{})},r&&QM(r)&&(i.resolve[hl]=r.title),i}var ll=class{url;params;queryParams;fragment;data;outlet;component;routeConfig;_resolve;_resolvedData;_routerState;_paramMap;_queryParamMap;_environmentInjector;get title(){return this.data?.[hl]}constructor(e,t,i,r,o,s,a,c,l,u){this.url=e,this.params=t,this.queryParams=i,this.fragment=r,this.data=o,this.outlet=s,this.component=a,this.routeConfig=c,this._resolve=l,this._environmentInjector=u}get root(){return this._routerState.root}get parent(){return this._routerState.parent(this)}get firstChild(){return this._routerState.firstChild(this)}get children(){return this._routerState.children(this)}get pathFromRoot(){return this._routerState.pathFromRoot(this)}get paramMap(){return this._paramMap??=ha(this.params),this._paramMap}get queryParamMap(){return this._queryParamMap??=ha(this.queryParams),this._queryParamMap}toString(){let e=this.url.map(i=>i.toString()).join("/"),t=this.routeConfig?this.routeConfig.path:"";return`Route(url:'${e}', path:'${t}')`}},nf=class extends ef{url;constructor(e,t){super(t),this.url=e,Wv(this,t)}toString(){return JM(this._root)}};function Wv(n,e){e.value._routerState=n,e.children.forEach(t=>Wv(n,t))}function JM(n){let e=n.children.length>0?` { ${n.children.map(JM).join(", ")} } `:"";return`${n.value}${e}`}function mv(n){if(n.snapshot){let e=n.snapshot,t=n._futureSnapshot;n.snapshot=t,sr(e.queryParams,t.queryParams)||n.queryParamsSubject.next(t.queryParams),e.fragment!==t.fragment&&n.fragmentSubject.next(t.fragment),sr(e.params,t.params)||n.paramsSubject.next(t.params),$R(e.url,t.url)||n.urlSubject.next(t.url),sr(e.data,t.data)||n.dataSubject.next(t.data)}else n.snapshot=n._futureSnapshot,n.dataSubject.next(n._futureSnapshot.data)}function Ov(n,e){let t=sr(n.params,e.params)&&KR(n.url,e.url),i=!n.parent!=!e.parent;return t&&!i&&(!n.parent||Ov(n.parent,e.parent))}function QM(n){return typeof n.title=="string"||n.title===null}var xD=new Ve(""),eb=(()=>{class n{activated=null;get activatedComponentRef(){return this.activated}_activatedRoute=null;name=Ze;activateEvents=new Bn;deactivateEvents=new Bn;attachEvents=new Bn;detachEvents=new Bn;routerOutletData=dM();parentContexts=ce(pl);location=ce(oa);changeDetector=ce(fM);inputBinder=ce(af,{optional:!0});supportsBindingToComponentInputs=!0;ngOnChanges(t){if(t.name){let{firstChange:i,previousValue:r}=t.name;if(i)return;this.isTrackedInParentContexts(r)&&(this.deactivate(),this.parentContexts.onChildOutletDestroyed(r)),this.initializeOutletWithName()}}ngOnDestroy(){this.isTrackedInParentContexts(this.name)&&this.parentContexts.onChildOutletDestroyed(this.name),this.inputBinder?.unsubscribeFromRouteData(this)}isTrackedInParentContexts(t){return this.parentContexts.getContext(t)?.outlet===this}ngOnInit(){this.initializeOutletWithName()}initializeOutletWithName(){if(this.parentContexts.onChildOutletCreated(this.name,this),this.activated)return;let t=this.parentContexts.getContext(this.name);t?.route&&(t.attachRef?this.attach(t.attachRef,t.route):this.activateWith(t.route,t.injector))}get isActivated(){return!!this.activated}get component(){if(!this.activated)throw new Oe(4012,!1);return this.activated.instance}get activatedRoute(){if(!this.activated)throw new Oe(4012,!1);return this._activatedRoute}get activatedRouteData(){return this._activatedRoute?this._activatedRoute.snapshot.data:{}}detach(){if(!this.activated)throw new Oe(4012,!1);this.location.detach();let t=this.activated;return this.activated=null,this._activatedRoute=null,this.detachEvents.emit(t.instance),t}attach(t,i){this.activated=t,this._activatedRoute=i,this.location.insert(t.hostView),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.attachEvents.emit(t.instance)}deactivate(){if(this.activated){let t=this.component;this.activated.destroy(),this.activated=null,this._activatedRoute=null,this.deactivateEvents.emit(t)}}activateWith(t,i){if(this.isActivated)throw new Oe(4013,!1);this._activatedRoute=t;let r=this.location,s=t.snapshot.component,a=this.parentContexts.getOrCreateContext(this.name).children,c=new Fv(t,a,r.injector,this.routerOutletData);this.activated=r.createComponent(s,{index:r.length,injector:c,environmentInjector:i}),this.changeDetector.markForCheck(),this.inputBinder?.bindActivatedRouteToOutletComponent(this),this.activateEvents.emit(this.activated.instance)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=$g({type:n,selectors:[["router-outlet"]],inputs:{name:"name",routerOutletData:[1,"routerOutletData"]},outputs:{activateEvents:"activate",deactivateEvents:"deactivate",attachEvents:"attach",detachEvents:"detach"},exportAs:["outlet"],features:[Tg]})}return n})(),Fv=class{route;childContexts;parent;outletData;constructor(e,t,i,r){this.route=e,this.childContexts=t,this.parent=i,this.outletData=r}get(e,t){return e===ss?this.route:e===pl?this.childContexts:e===xD?this.outletData:this.parent.get(e,t)}},af=new Ve("");var tb=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=ns({type:n,selectors:[["ng-component"]],exportAs:["emptyRouterOutlet"],decls:1,vars:0,template:function(i,r){i&1&&aa(0,"router-outlet")},dependencies:[eb],encapsulation:2})}return n})();function jv(n){let e=n.children&&n.children.map(jv),t=e?Ut(Ee({},n),{children:e}):Ee({},n);return!t.component&&!t.loadComponent&&(e||t.loadChildren)&&t.outlet&&t.outlet!==Ze&&(t.component=tb),t}function ED(n,e,t){let i=ul(n,e._root,t?t._root:void 0);return new tf(i,e)}function ul(n,e,t){if(t&&n.shouldReuseRoute(e.value,t.value.snapshot)){let i=t.value;i._futureSnapshot=e.value;let r=MD(n,e,t);return new ri(i,r)}else{if(n.shouldAttach(e.value)){let o=n.retrieve(e.value);if(o!==null){let s=o.route;return s.value._futureSnapshot=e.value,s.children=e.children.map(a=>ul(n,a)),s}}let i=bD(e.value),r=e.children.map(o=>ul(n,o));return new ri(i,r)}}function MD(n,e,t){return e.children.map(i=>{for(let r of t.children)if(n.shouldReuseRoute(i.value,r.value.snapshot))return ul(n,i,r);return ul(n,i)})}function bD(n){return new ss(new bn(n.url),new bn(n.params),new bn(n.queryParams),new bn(n.fragment),new bn(n.data),n.outlet,n.component,n)}var dl=class{redirectTo;navigationBehaviorOptions;constructor(e,t){this.redirectTo=e,this.navigationBehaviorOptions=t}},nb="ngNavigationCancelingError";function rf(n,e){let{redirectTo:t,navigationBehaviorOptions:i}=pa(e)?{redirectTo:e,navigationBehaviorOptions:void 0}:e,r=ib(!1,Hn.Redirect);return r.url=t,r.navigationBehaviorOptions=i,r}function ib(n,e){let t=new Error(`NavigationCancelingError: ${n||""}`);return t[nb]=!0,t.cancellationCode=e,t}function SD(n){return rb(n)&&pa(n.url)}function rb(n){return!!n&&n[nb]}var kv=class{routeReuseStrategy;futureState;currState;forwardEvent;inputBindingEnabled;constructor(e,t,i,r,o){this.routeReuseStrategy=e,this.futureState=t,this.currState=i,this.forwardEvent=r,this.inputBindingEnabled=o}activate(e){let t=this.futureState._root,i=this.currState?this.currState._root:null;this.deactivateChildRoutes(t,i,e),mv(this.futureState.root),this.activateChildRoutes(t,i,e)}deactivateChildRoutes(e,t,i){let r=ua(t);e.children.forEach(o=>{let s=o.value.outlet;this.deactivateRoutes(o,r[s],i),delete r[s]}),Object.values(r).forEach(o=>{this.deactivateRouteAndItsChildren(o,i)})}deactivateRoutes(e,t,i){let r=e.value,o=t?t.value:null;if(r===o)if(r.component){let s=i.getContext(r.outlet);s&&this.deactivateChildRoutes(e,t,s.children)}else this.deactivateChildRoutes(e,t,i);else o&&this.deactivateRouteAndItsChildren(t,i)}deactivateRouteAndItsChildren(e,t){e.value.component&&this.routeReuseStrategy.shouldDetach(e.value.snapshot)?this.detachAndStoreRouteSubtree(e,t):this.deactivateRouteAndOutlet(e,t)}detachAndStoreRouteSubtree(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,o=ua(e);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);if(i&&i.outlet){let s=i.outlet.detach(),a=i.children.onOutletDeactivated();this.routeReuseStrategy.store(e.value.snapshot,{componentRef:s,route:e,contexts:a})}}deactivateRouteAndOutlet(e,t){let i=t.getContext(e.value.outlet),r=i&&e.value.component?i.children:t,o=ua(e);for(let s of Object.values(o))this.deactivateRouteAndItsChildren(s,r);i&&(i.outlet&&(i.outlet.deactivate(),i.children.onOutletDeactivated()),i.attachRef=null,i.route=null)}activateChildRoutes(e,t,i){let r=ua(t);e.children.forEach(o=>{this.activateRoutes(o,r[o.value.outlet],i),this.forwardEvent(new Dv(o.value.snapshot))}),e.children.length&&this.forwardEvent(new Av(e.value.snapshot))}activateRoutes(e,t,i){let r=e.value,o=t?t.value:null;if(mv(r),r===o)if(r.component){let s=i.getOrCreateContext(r.outlet);this.activateChildRoutes(e,t,s.children)}else this.activateChildRoutes(e,t,i);else if(r.component){let s=i.getOrCreateContext(r.outlet);if(this.routeReuseStrategy.shouldAttach(r.snapshot)){let a=this.routeReuseStrategy.retrieve(r.snapshot);this.routeReuseStrategy.store(r.snapshot,null),s.children.onOutletReAttached(a.contexts),s.attachRef=a.componentRef,s.route=a.route.value,s.outlet&&s.outlet.attach(a.componentRef,a.route.value),mv(a.route.value),this.activateChildRoutes(e,null,s.children)}else s.attachRef=null,s.route=r,s.outlet&&s.outlet.activateWith(r,s.injector),this.activateChildRoutes(e,null,s.children)}else this.activateChildRoutes(e,null,i)}},of=class{path;route;constructor(e){this.path=e,this.route=this.path[this.path.length-1]}},fa=class{component;route;constructor(e,t){this.component=e,this.route=t}};function wD(n,e,t){let i=n._root,r=e?e._root:null;return il(i,r,t,[i.value])}function TD(n){let e=n.routeConfig?n.routeConfig.canActivateChild:null;return!e||e.length===0?null:{node:n,guards:e}}function xa(n,e){let t=Symbol(),i=e.get(n,t);return i===t?typeof n=="function"&&!cm(n)?n:e.get(n):i}function il(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=ua(e);return n.children.forEach(s=>{CD(s,o[s.value.outlet],t,i.concat([s.value]),r),delete o[s.value.outlet]}),Object.entries(o).forEach(([s,a])=>sl(a,t.getContext(s),r)),r}function CD(n,e,t,i,r={canDeactivateChecks:[],canActivateChecks:[]}){let o=n.value,s=e?e.value:null,a=t?t.getContext(n.value.outlet):null;if(s&&o.routeConfig===s.routeConfig){let c=ID(s,o,o.routeConfig.runGuardsAndResolvers);c?r.canActivateChecks.push(new of(i)):(o.data=s.data,o._resolvedData=s._resolvedData),o.component?il(n,e,a?a.children:null,i,r):il(n,e,t,i,r),c&&a&&a.outlet&&a.outlet.isActivated&&r.canDeactivateChecks.push(new fa(a.outlet.component,s))}else s&&sl(e,a,r),r.canActivateChecks.push(new of(i)),o.component?il(n,null,a?a.children:null,i,r):il(n,null,t,i,r);return r}function ID(n,e,t){if(typeof t=="function")return An(e._environmentInjector,()=>t(n,e));switch(t){case"pathParamsChange":return!rs(n.url,e.url);case"pathParamsOrQueryParamsChange":return!rs(n.url,e.url)||!sr(n.queryParams,e.queryParams);case"always":return!0;case"paramsOrQueryParamsChange":return!Ov(n,e)||!sr(n.queryParams,e.queryParams);default:return!Ov(n,e)}}function sl(n,e,t){let i=ua(n),r=n.value;Object.entries(i).forEach(([o,s])=>{r.component?e?sl(s,e.children.getContext(o),t):sl(s,null,t):sl(s,e,t)}),r.component?e&&e.outlet&&e.outlet.isActivated?t.canDeactivateChecks.push(new fa(e.outlet.component,r)):t.canDeactivateChecks.push(new fa(null,r)):t.canDeactivateChecks.push(new fa(null,r))}function ml(n){return typeof n=="function"}function AD(n){return typeof n=="boolean"}function RD(n){return n&&ml(n.canLoad)}function DD(n){return n&&ml(n.canActivate)}function ND(n){return n&&ml(n.canActivateChild)}function PD(n){return n&&ml(n.canDeactivate)}function LD(n){return n&&ml(n.canMatch)}function ob(n){return n instanceof Bo||n?.name==="EmptyError"}var jd=Symbol("INITIAL_VALUE");function _a(){return Li(n=>Vp(n.map(e=>e.pipe(Er(1),Gp(jd)))).pipe(qt(e=>{for(let t of e)if(t!==!0){if(t===jd)return jd;if(t===!1||OD(t))return t}return!0}),xr(e=>e!==jd),Er(1)))}function OD(n){return pa(n)||n instanceof dl}function sb(n){return n.aborted?dt(void 0).pipe(Er(1)):new Mt(e=>{let t=()=>{e.next(),e.complete()};return n.addEventListener("abort",t),()=>n.removeEventListener("abort",t)})}function ab(n){return mc(sb(n))}function FD(n){return Un(e=>{let{targetSnapshot:t,currentSnapshot:i,guards:{canActivateChecks:r,canDeactivateChecks:o}}=e;return o.length===0&&r.length===0?dt(Ut(Ee({},e),{guardsResult:!0})):kD(o,t,i).pipe(Un(s=>s&&AD(s)?UD(t,r,n):dt(s)),qt(s=>Ut(Ee({},e),{guardsResult:s})))})}function kD(n,e,t){return tn(n).pipe(Un(i=>GD(i.component,i.route,t,e)),Mr(i=>i!==!0,!0))}function UD(n,e,t){return tn(e).pipe(Cu(i=>Fs(HD(i.route.parent,t),BD(i.route,t),zD(n,i.path),VD(n,i.route))),Mr(i=>i!==!0,!0))}function BD(n,e){return n!==null&&e&&e(new Rv(n)),dt(!0)}function HD(n,e){return n!==null&&e&&e(new Iv(n)),dt(!0)}function VD(n,e){let t=e.routeConfig?e.routeConfig.canActivate:null;if(!t||t.length===0)return dt(!0);let i=t.map(r=>hc(()=>{let o=e._environmentInjector,s=xa(r,o),a=DD(s)?s.canActivate(e,n):An(o,()=>s(e,n));return as(a).pipe(Mr())}));return dt(i).pipe(_a())}function zD(n,e){let t=e[e.length-1],r=e.slice(0,e.length-1).reverse().map(o=>TD(o)).filter(o=>o!==null).map(o=>hc(()=>{let s=o.guards.map(a=>{let c=o.node._environmentInjector,l=xa(a,c),u=ND(l)?l.canActivateChild(t,n):An(c,()=>l(t,n));return as(u).pipe(Mr())});return dt(s).pipe(_a())}));return dt(r).pipe(_a())}function GD(n,e,t,i){let r=e&&e.routeConfig?e.routeConfig.canDeactivate:null;if(!r||r.length===0)return dt(!0);let o=r.map(s=>{let a=e._environmentInjector,c=xa(s,a),l=PD(c)?c.canDeactivate(n,e,t,i):An(a,()=>c(n,e,t,i));return as(l).pipe(Mr())});return dt(o).pipe(_a())}function WD(n,e,t,i,r){let o=e.canLoad;if(o===void 0||o.length===0)return dt(!0);let s=o.map(a=>{let c=xa(a,n),l=RD(c)?c.canLoad(e,t):An(n,()=>c(e,t)),u=as(l);return r?u.pipe(ab(r)):u});return dt(s).pipe(_a(),cb(i))}function cb(n){return kp(gi(e=>{if(typeof e!="boolean")throw rf(n,e)}),qt(e=>e===!0))}function jD(n,e,t,i,r,o){let s=e.canMatch;if(!s||s.length===0)return dt(!0);let a=s.map(c=>{let l=xa(c,n),u=LD(l)?l.canMatch(e,t,r):An(n,()=>l(e,t,r));return as(u).pipe(ab(o))});return dt(a).pipe(_a(),cb(i))}var Ir=class n extends Error{segmentGroup;constructor(e){super(),this.segmentGroup=e||null,Object.setPrototypeOf(this,n.prototype)}},fl=class n extends Error{urlTree;constructor(e){super(),this.urlTree=e,Object.setPrototypeOf(this,n.prototype)}};function $D(n){throw new Oe(4e3,!1)}function qD(n){throw ib(!1,Hn.GuardRejected)}var Uv=class{urlSerializer;urlTree;constructor(e,t){this.urlSerializer=e,this.urlTree=t}async lineralizeSegments(e,t){let i=[],r=t.root;for(;;){if(i=i.concat(r.segments),r.numberOfChildren===0)return i;if(r.numberOfChildren>1||!r.children[Ze])throw $D(`${e.redirectTo}`);r=r.children[Ze]}}async applyRedirectCommands(e,t,i,r,o){let s=await XD(t,r,o);if(s instanceof zi)throw new fl(s);let a=this.applyRedirectCreateUrlTree(s,this.urlSerializer.parse(s),e,i);if(s[0]==="/")throw new fl(a);return a}applyRedirectCreateUrlTree(e,t,i,r){let o=this.createSegmentGroup(e,t.root,i,r);return new zi(o,this.createQueryParams(t.queryParams,this.urlTree.queryParams),t.fragment)}createQueryParams(e,t){let i={};return Object.entries(e).forEach(([r,o])=>{if(typeof o=="string"&&o[0]===":"){let a=o.substring(1);i[r]=t[a]}else i[r]=o}),i}createSegmentGroup(e,t,i,r){let o=this.createSegments(e,t.segments,i,r),s={};return Object.entries(t.children).forEach(([a,c])=>{s[a]=this.createSegmentGroup(e,c,i,r)}),new It(o,s)}createSegments(e,t,i,r){return t.map(o=>o.path[0]===":"?this.findPosParam(e,o,r):this.findOrReturn(o,i))}findPosParam(e,t,i){let r=i[t.path.substring(1)];if(!r)throw new Oe(4001,!1);return r}findOrReturn(e,t){let i=0;for(let r of t){if(r.path===e.path)return t.splice(i),r;i++}return e}};function XD(n,e,t){if(typeof n=="string")return Promise.resolve(n);let i=n;return Yd(as(An(t,()=>i(e))))}function YD(n,e){return n.providers&&!n._injector&&(n._injector=Wc(n.providers,e,`Route: ${n.path}`)),n._injector??e}function ar(n){return n.outlet||Ze}function ZD(n,e){let t=n.filter(i=>ar(i)===e);return t.push(...n.filter(i=>ar(i)!==e)),t}var Bv={matched:!1,consumedSegments:[],remainingSegments:[],parameters:{},positionalParamSegments:{}};function lb(n){return{routeConfig:n.routeConfig,url:n.url,params:n.params,queryParams:n.queryParams,fragment:n.fragment,data:n.data,outlet:n.outlet,title:n.title,paramMap:n.paramMap,queryParamMap:n.queryParamMap}}function KD(n,e,t,i,r,o,s){let a=ub(n,e,t);if(!a.matched)return dt(a);let c=lb(o(a));return i=YD(e,i),jD(i,e,t,r,c,s).pipe(qt(l=>l===!0?a:Ee({},Bv)))}function ub(n,e,t){if(e.path==="")return e.pathMatch==="full"&&(n.hasChildren()||t.length>0)?Ee({},Bv):{matched:!0,consumedSegments:[],remainingSegments:t,parameters:{},positionalParamSegments:{}};let r=(e.matcher||jR)(t,n,e);if(!r)return Ee({},Bv);let o={};Object.entries(r.posParams??{}).forEach(([a,c])=>{o[a]=c.path});let s=r.consumed.length>0?Ee(Ee({},o),r.consumed[r.consumed.length-1].parameters):o;return{matched:!0,consumedSegments:r.consumed,remainingSegments:t.slice(r.consumed.length),parameters:s,positionalParamSegments:r.posParams??{}}}function PM(n,e,t,i){return t.length>0&&e1(n,t,i)?{segmentGroup:new It(e,QD(i,new It(t,n.children))),slicedSegments:[]}:t.length===0&&t1(n,t,i)?{segmentGroup:new It(n.segments,JD(n,t,i,n.children)),slicedSegments:t}:{segmentGroup:new It(n.segments,n.children),slicedSegments:t}}function JD(n,e,t,i){let r={};for(let o of t)if(cf(n,e,o)&&!i[ar(o)]){let s=new It([],{});r[ar(o)]=s}return Ee(Ee({},i),r)}function QD(n,e){let t={};t[Ze]=e;for(let i of n)if(i.path===""&&ar(i)!==Ze){let r=new It([],{});t[ar(i)]=r}return t}function e1(n,e,t){return t.some(i=>cf(n,e,i)&&ar(i)!==Ze)}function t1(n,e,t){return t.some(i=>cf(n,e,i))}function cf(n,e,t){return(n.hasChildren()||e.length>0)&&t.pathMatch==="full"?!1:t.path===""}function n1(n,e,t){return e.length===0&&!n.children[t]}var Hv=class{};async function i1(n,e,t,i,r,o,s="emptyOnly",a){return new Vv(n,e,t,i,r,s,o,a).recognize()}var r1=31,Vv=class{injector;configLoader;rootComponentType;config;urlTree;paramsInheritanceStrategy;urlSerializer;abortSignal;applyRedirects;absoluteRedirectCount=0;allowRedirects=!0;constructor(e,t,i,r,o,s,a,c){this.injector=e,this.configLoader=t,this.rootComponentType=i,this.config=r,this.urlTree=o,this.paramsInheritanceStrategy=s,this.urlSerializer=a,this.abortSignal=c,this.applyRedirects=new Uv(this.urlSerializer,this.urlTree)}noMatchError(e){return new Oe(4002,`'${e.segmentGroup}'`)}async recognize(){let e=PM(this.urlTree.root,[],[],this.config).segmentGroup,{children:t,rootSnapshot:i}=await this.match(e),r=new ri(i,t),o=new nf("",r),s=dD(i,[],this.urlTree.queryParams,this.urlTree.fragment);return s.queryParams=this.urlTree.queryParams,o.url=this.urlSerializer.serialize(s),{state:o,tree:s}}async match(e){let t=new ll([],Object.freeze({}),Object.freeze(Ee({},this.urlTree.queryParams)),this.urlTree.fragment,Object.freeze({}),Ze,this.rootComponentType,null,{},this.injector);try{return{children:await this.processSegmentGroup(this.injector,this.config,e,Ze,t),rootSnapshot:t}}catch(i){if(i instanceof fl)return this.urlTree=i.urlTree,this.match(i.urlTree.root);throw i instanceof Ir?this.noMatchError(i):i}}async processSegmentGroup(e,t,i,r,o){if(i.segments.length===0&&i.hasChildren())return this.processChildren(e,t,i,o);let s=await this.processSegment(e,t,i,i.segments,r,!0,o);return s instanceof ri?[s]:[]}async processChildren(e,t,i,r){let o=[];for(let c of Object.keys(i.children))c==="primary"?o.unshift(c):o.push(c);let s=[];for(let c of o){let l=i.children[c],u=ZD(t,c),d=await this.processSegmentGroup(e,u,l,c,r);s.push(...d)}let a=db(s);return o1(a),a}async processSegment(e,t,i,r,o,s,a){for(let c of t)try{return await this.processSegmentAgainstRoute(c._injector??e,t,c,i,r,o,s,a)}catch(l){if(l instanceof Ir||ob(l))continue;throw l}if(n1(i,r,o))return new Hv;throw new Ir(i)}async processSegmentAgainstRoute(e,t,i,r,o,s,a,c){if(ar(i)!==s&&(s===Ze||!cf(r,o,i)))throw new Ir(r);if(i.redirectTo===void 0)return this.matchSegmentAgainstRoute(e,r,i,o,s,c);if(this.allowRedirects&&a)return this.expandSegmentAgainstRouteUsingRedirect(e,r,t,i,o,s,c);throw new Ir(r)}async expandSegmentAgainstRouteUsingRedirect(e,t,i,r,o,s,a){let{matched:c,parameters:l,consumedSegments:u,positionalParamSegments:d,remainingSegments:f}=ub(t,r,o);if(!c)throw new Ir(t);typeof r.redirectTo=="string"&&r.redirectTo[0]==="/"&&(this.absoluteRedirectCount++,this.absoluteRedirectCount>r1&&(this.allowRedirects=!1));let h=this.createSnapshot(e,r,o,l,a);if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let m=await this.applyRedirects.applyRedirectCommands(u,r.redirectTo,d,lb(h),e),y=await this.applyRedirects.lineralizeSegments(r,m);return this.processSegment(e,i,t,y.concat(f),s,!1,a)}createSnapshot(e,t,i,r,o){let s=new ll(i,r,Object.freeze(Ee({},this.urlTree.queryParams)),this.urlTree.fragment,a1(t),ar(t),t.component??t._loadedComponent??null,t,c1(t),e),a=Gv(s,o,this.paramsInheritanceStrategy);return s.params=Object.freeze(a.params),s.data=Object.freeze(a.data),s}async matchSegmentAgainstRoute(e,t,i,r,o,s){if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);let a=w=>this.createSnapshot(e,i,w.consumedSegments,w.parameters,s),c=await Yd(KD(t,i,r,e,this.urlSerializer,a,this.abortSignal));if(i.path==="**"&&(t.children={}),!c?.matched)throw new Ir(t);e=i._injector??e;let{routes:l}=await this.getChildConfig(e,i,r),u=i._loadedInjector??e,{parameters:d,consumedSegments:f,remainingSegments:h}=c,m=this.createSnapshot(e,i,f,d,s),{segmentGroup:y,slicedSegments:g}=PM(t,f,h,l);if(g.length===0&&y.hasChildren()){let w=await this.processChildren(u,l,y,m);return new ri(m,w)}if(l.length===0&&g.length===0)return new ri(m,[]);let p=ar(i)===o,E=await this.processSegment(u,l,y,g,p?Ze:o,!0,m);return new ri(m,E instanceof ri?[E]:[])}async getChildConfig(e,t,i){if(t.children)return{routes:t.children,injector:e};if(t.loadChildren){if(t._loadedRoutes!==void 0){let o=t._loadedNgModuleFactory;return o&&!t._loadedInjector&&(t._loadedInjector=o.create(e).injector),{routes:t._loadedRoutes,injector:t._loadedInjector}}if(this.abortSignal.aborted)throw new Error(this.abortSignal.reason);if(await Yd(WD(e,t,i,this.urlSerializer,this.abortSignal))){let o=await this.configLoader.loadChildren(e,t);return t._loadedRoutes=o.routes,t._loadedInjector=o.injector,t._loadedNgModuleFactory=o.factory,o}throw qD(t)}return{routes:[],injector:e}}};function o1(n){n.sort((e,t)=>e.value.outlet===Ze?-1:t.value.outlet===Ze?1:e.value.outlet.localeCompare(t.value.outlet))}function s1(n){let e=n.value.routeConfig;return e&&e.path===""}function db(n){let e=[],t=new Set;for(let i of n){if(!s1(i)){e.push(i);continue}let r=e.find(o=>i.value.routeConfig===o.value.routeConfig);r!==void 0?(r.children.push(...i.children),t.add(r)):e.push(i)}for(let i of t){let r=db(i.children);e.push(new ri(i.value,r))}return e.filter(i=>!t.has(i))}function a1(n){return n.data||{}}function c1(n){return n.resolve||{}}function l1(n,e,t,i,r,o,s){return Un(async a=>{let{state:c,tree:l}=await i1(n,e,t,i,a.extractedUrl,r,o,s);return Ut(Ee({},a),{targetSnapshot:c,urlAfterRedirects:l})})}function u1(n){return Un(e=>{let{targetSnapshot:t,guards:{canActivateChecks:i}}=e;if(!i.length)return dt(e);let r=new Set(i.map(a=>a.route)),o=new Set;for(let a of r)if(!o.has(a))for(let c of fb(a))o.add(c);let s=0;return tn(o).pipe(Cu(a=>r.has(a)?d1(a,t,n):(a.data=Gv(a,a.parent,n).resolve,dt(void 0))),gi(()=>s++),Iu(1),Un(a=>s===o.size?dt(e):Sn))})}function fb(n){let e=n.children.map(t=>fb(t)).flat();return[n,...e]}function d1(n,e,t){let i=n.routeConfig,r=n._resolve;return i?.title!==void 0&&!QM(i)&&(r[hl]=i.title),hc(()=>(n.data=Gv(n,n.parent,t).resolve,f1(r,n,e).pipe(qt(o=>(n._resolvedData=o,n.data=Ee(Ee({},n.data),o),null)))))}function f1(n,e,t){let i=vv(n);if(i.length===0)return dt({});let r={};return tn(i).pipe(Un(o=>h1(n[o],e,t).pipe(Mr(),gi(s=>{if(s instanceof dl)throw rf(new os,s);r[o]=s}))),Iu(1),qt(()=>r),pc(o=>ob(o)?Sn:Hp(o)))}function h1(n,e,t){let i=e._environmentInjector,r=xa(n,i),o=r.resolve?r.resolve(e,t):An(i,()=>r(e,t));return as(o)}function LM(n){return Li(e=>{let t=n(e);return t?tn(t).pipe(qt(()=>e)):dt(e)})}var hb=(()=>{class n{buildTitle(t){let i,r=t.root;for(;r!==void 0;)i=this.getResolvedTitleForRoute(r)??i,r=r.children.find(o=>o.outlet===Ze);return i}getResolvedTitleForRoute(t){return t.data[hl]}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>ce(p1),providedIn:"root"})}return n})(),p1=(()=>{class n extends hb{title;constructor(t){super(),this.title=t}updateTitle(t){let i=this.buildTitle(t);i!==void 0&&this.title.setTitle(i)}static \u0275fac=function(i){return new(i||n)(it(CM))};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),lf=new Ve("",{factory:()=>({})}),uf=new Ve(""),pb=(()=>{class n{componentLoaders=new WeakMap;childrenLoaders=new WeakMap;onLoadStartListener;onLoadEndListener;compiler=ce(Qg);async loadComponent(t,i){if(this.componentLoaders.get(i))return this.componentLoaders.get(i);if(i._loadedComponent)return Promise.resolve(i._loadedComponent);this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await FM(An(t,()=>i.loadComponent())),s=await gb(mb(o));return this.onLoadEndListener&&this.onLoadEndListener(i),i._loadedComponent=s,s}finally{this.componentLoaders.delete(i)}})();return this.componentLoaders.set(i,r),r}loadChildren(t,i){if(this.childrenLoaders.get(i))return this.childrenLoaders.get(i);if(i._loadedRoutes)return Promise.resolve({routes:i._loadedRoutes,injector:i._loadedInjector});this.onLoadStartListener&&this.onLoadStartListener(i);let r=(async()=>{try{let o=await m1(i,this.compiler,t,this.onLoadEndListener);return i._loadedRoutes=o.routes,i._loadedInjector=o.injector,i._loadedNgModuleFactory=o.factory,o}finally{this.childrenLoaders.delete(i)}})();return this.childrenLoaders.set(i,r),r}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();async function m1(n,e,t,i){let r=await FM(An(t,()=>n.loadChildren())),o=await gb(mb(r)),s;o instanceof Rd||Array.isArray(o)?s=o:s=await e.compileModuleAsync(o),i&&i(n);let a,c,l=!1,u;return Array.isArray(s)?(c=s,l=!0):(a=s.create(t).injector,u=s,c=a.get(uf,[],{optional:!0,self:!0}).flat()),{routes:c.map(jv),injector:a,factory:u}}function g1(n){return n&&typeof n=="object"&&"default"in n}function mb(n){return g1(n)?n.default:n}async function gb(n){return n}var $v=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>ce(v1),providedIn:"root"})}return n})(),v1=(()=>{class n{shouldProcessUrl(t){return!0}extract(t){return t}merge(t,i){return t}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),vb=new Ve("");var y1=()=>{},yb=new Ve(""),_b=(()=>{class n{currentNavigation=Nn(null,{equal:()=>!1});currentTransition=null;lastSuccessfulNavigation=Nn(null);events=new Mn;transitionAbortWithErrorSubject=new Mn;configLoader=ce(pb);environmentInjector=ce(rn);destroyRef=ce(eo);urlSerializer=ce(sf);rootContexts=ce(pl);location=ce(la);inputBindingEnabled=ce(af,{optional:!0})!==null;titleStrategy=ce(hb);options=ce(lf,{optional:!0})||{};paramsInheritanceStrategy=this.options.paramsInheritanceStrategy||"emptyOnly";urlHandlingStrategy=ce($v);createViewTransition=ce(vb,{optional:!0});navigationErrorHandler=ce(yb,{optional:!0});navigationId=0;get hasRequestedNavigation(){return this.navigationId!==0}transitions;afterPreactivation=()=>dt(void 0);rootComponentType=null;destroyed=!1;constructor(){let t=r=>this.events.next(new Tv(r)),i=r=>this.events.next(new Cv(r));this.configLoader.onLoadEndListener=i,this.configLoader.onLoadStartListener=t,this.destroyRef.onDestroy(()=>{this.destroyed=!0})}complete(){this.transitions?.complete()}handleNavigationRequest(t){let i=++this.navigationId;so(()=>{this.transitions?.next(Ut(Ee({},t),{extractedUrl:this.urlHandlingStrategy.extract(t.rawUrl),targetSnapshot:null,targetRouterState:null,guards:{canActivateChecks:[],canDeactivateChecks:[]},guardsResult:null,id:i,routesRecognizeHandler:{},beforeActivateHandler:{}}))})}setupNavigations(t){return this.transitions=new bn(null),this.transitions.pipe(xr(i=>i!==null),Li(i=>{let r=!1,o=new AbortController,s=()=>!r&&this.currentTransition?.id===i.id;return dt(i).pipe(Li(a=>{if(this.navigationId>i.id)return this.cancelNavigationTransition(i,"",Hn.SupersededByNewNavigation),Sn;this.currentTransition=i;let c=this.lastSuccessfulNavigation();this.currentNavigation.set({id:a.id,initialUrl:a.rawUrl,extractedUrl:a.extractedUrl,targetBrowserUrl:typeof a.extras.browserUrl=="string"?this.urlSerializer.parse(a.extras.browserUrl):a.extras.browserUrl,trigger:a.source,extras:a.extras,previousNavigation:c?Ut(Ee({},c),{previousNavigation:null}):null,abort:()=>o.abort(),routesRecognizeHandler:a.routesRecognizeHandler,beforeActivateHandler:a.beforeActivateHandler});let l=!t.navigated||this.isUpdatingInternalState()||this.isUpdatedBrowserUrl(),u=a.extras.onSameUrlNavigation??t.onSameUrlNavigation;if(!l&&u!=="reload")return this.events.next(new uo(a.id,this.urlSerializer.serialize(a.rawUrl),"",Jd.IgnoredSameUrlNavigation)),a.resolve(!1),Sn;if(this.urlHandlingStrategy.shouldProcessUrl(a.rawUrl))return dt(a).pipe(Li(d=>(this.events.next(new ma(d.id,this.urlSerializer.serialize(d.extractedUrl),d.source,d.restoredState)),d.id!==this.navigationId?Sn:Promise.resolve(d))),l1(this.environmentInjector,this.configLoader,this.rootComponentType,t.config,this.urlSerializer,this.paramsInheritanceStrategy,o.signal),gi(d=>{i.targetSnapshot=d.targetSnapshot,i.urlAfterRedirects=d.urlAfterRedirects,this.currentNavigation.update(f=>(f.finalUrl=d.urlAfterRedirects,f)),this.events.next(new cl)}),Li(d=>tn(i.routesRecognizeHandler.deferredHandle??dt(void 0)).pipe(qt(()=>d))),gi(()=>{let d=new Qd(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)}));if(l&&this.urlHandlingStrategy.shouldProcessUrl(a.currentRawUrl)){let{id:d,extractedUrl:f,source:h,restoredState:m,extras:y}=a,g=new ma(d,this.urlSerializer.serialize(f),h,m);this.events.next(g);let p=KM(this.rootComponentType,this.environmentInjector).snapshot;return this.currentTransition=i=Ut(Ee({},a),{targetSnapshot:p,urlAfterRedirects:f,extras:Ut(Ee({},y),{skipLocationChange:!1,replaceUrl:!1})}),this.currentNavigation.update(E=>(E.finalUrl=f,E)),dt(i)}else return this.events.next(new uo(a.id,this.urlSerializer.serialize(a.extractedUrl),"",Jd.IgnoredByUrlHandlingStrategy)),a.resolve(!1),Sn}),qt(a=>{let c=new Mv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);return this.events.next(c),this.currentTransition=i=Ut(Ee({},a),{guards:wD(a.targetSnapshot,a.currentSnapshot,this.rootContexts)}),i}),FD(a=>this.events.next(a)),Li(a=>{if(i.guardsResult=a.guardsResult,a.guardsResult&&typeof a.guardsResult!="boolean")throw rf(this.urlSerializer,a.guardsResult);let c=new bv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot,!!a.guardsResult);if(this.events.next(c),!s())return Sn;if(!a.guardsResult)return this.cancelNavigationTransition(a,"",Hn.GuardRejected),Sn;if(a.guards.canActivateChecks.length===0)return dt(a);let l=new Sv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);if(this.events.next(l),!s())return Sn;let u=!1;return dt(a).pipe(u1(this.paramsInheritanceStrategy),gi({next:()=>{u=!0;let d=new wv(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects),a.targetSnapshot);this.events.next(d)},complete:()=>{u||this.cancelNavigationTransition(a,"",Hn.NoDataFromResolver)}}))}),LM(a=>{let c=u=>{let d=[];if(u.routeConfig?._loadedComponent)u.component=u.routeConfig?._loadedComponent;else if(u.routeConfig?.loadComponent){let f=u._environmentInjector;d.push(this.configLoader.loadComponent(f,u.routeConfig).then(h=>{u.component=h}))}for(let f of u.children)d.push(...c(f));return d},l=c(a.targetSnapshot.root);return l.length===0?dt(a):tn(Promise.all(l).then(()=>a))}),LM(()=>this.afterPreactivation()),Li(()=>{let{currentSnapshot:a,targetSnapshot:c}=i,l=this.createViewTransition?.(this.environmentInjector,a.root,c.root);return l?tn(l).pipe(qt(()=>i)):dt(i)}),Er(1),Li(a=>{let c=ED(t.routeReuseStrategy,a.targetSnapshot,a.currentRouterState);this.currentTransition=i=a=Ut(Ee({},a),{targetRouterState:c}),this.currentNavigation.update(u=>(u.targetRouterState=c,u)),this.events.next(new va);let l=i.beforeActivateHandler.deferredHandle;return l?tn(l.then(()=>a)):dt(a)}),gi(a=>{new kv(t.routeReuseStrategy,i.targetRouterState,i.currentRouterState,c=>this.events.next(c),this.inputBindingEnabled).activate(this.rootContexts),s()&&(r=!0,this.currentNavigation.update(c=>(c.abort=y1,c)),this.lastSuccessfulNavigation.set(so(this.currentNavigation)),this.events.next(new lo(a.id,this.urlSerializer.serialize(a.extractedUrl),this.urlSerializer.serialize(a.urlAfterRedirects))),this.titleStrategy?.updateTitle(a.targetRouterState.snapshot),a.resolve(!0))}),mc(sb(o.signal).pipe(xr(()=>!r&&!i.targetRouterState),gi(()=>{this.cancelNavigationTransition(i,o.signal.reason+"",Hn.Aborted)}))),gi({complete:()=>{r=!0}}),mc(this.transitionAbortWithErrorSubject.pipe(gi(a=>{throw a}))),zp(()=>{o.abort(),r||this.cancelNavigationTransition(i,"",Hn.SupersededByNewNavigation),this.currentTransition?.id===i.id&&(this.currentNavigation.set(null),this.currentTransition=null)}),pc(a=>{if(r=!0,this.destroyed)return i.resolve(!1),Sn;if(rb(a))this.events.next(new Vi(i.id,this.urlSerializer.serialize(i.extractedUrl),a.message,a.cancellationCode)),SD(a)?this.events.next(new ya(a.url,a.navigationBehaviorOptions)):i.resolve(!1);else{let c=new ga(i.id,this.urlSerializer.serialize(i.extractedUrl),a,i.targetSnapshot??void 0);try{let l=An(this.environmentInjector,()=>this.navigationErrorHandler?.(c));if(l instanceof dl){let{message:u,cancellationCode:d}=rf(this.urlSerializer,l);this.events.next(new Vi(i.id,this.urlSerializer.serialize(i.extractedUrl),u,d)),this.events.next(new ya(l.redirectTo,l.navigationBehaviorOptions))}else throw this.events.next(c),a}catch(l){this.options.resolveNavigationPromiseOnError?i.resolve(!1):i.reject(l)}}return Sn}))}))}cancelNavigationTransition(t,i,r){let o=new Vi(t.id,this.urlSerializer.serialize(t.extractedUrl),i,r);this.events.next(o),t.resolve(!1)}isUpdatingInternalState(){return this.currentTransition?.extractedUrl.toString()!==this.currentTransition?.currentUrlTree.toString()}isUpdatedBrowserUrl(){let t=this.urlHandlingStrategy.extract(this.urlSerializer.parse(this.location.path(!0))),i=so(this.currentNavigation),r=i?.targetBrowserUrl??i?.extractedUrl;return t.toString()!==r?.toString()&&!i?.extras.skipLocationChange}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function _1(n){return n!==ol}var xb=new Ve("");var x1=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>ce(E1),providedIn:"root"})}return n})(),zv=class{shouldDetach(e){return!1}store(e,t){}shouldAttach(e){return!1}retrieve(e){return null}shouldReuseRoute(e,t){return e.routeConfig===t.routeConfig}shouldDestroyInjector(e){return!0}},E1=(()=>{class n extends zv{static \u0275fac=(()=>{let t;return function(r){return(t||(t=Hc(n)))(r||n)}})();static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),qv=(()=>{class n{urlSerializer=ce(sf);options=ce(lf,{optional:!0})||{};canceledNavigationResolution=this.options.canceledNavigationResolution||"replace";location=ce(la);urlHandlingStrategy=ce($v);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";currentUrlTree=new zi;getCurrentUrlTree(){return this.currentUrlTree}rawUrlTree=this.currentUrlTree;getRawUrlTree(){return this.rawUrlTree}createBrowserPath({finalUrl:t,initialUrl:i,targetBrowserUrl:r}){let o=t!==void 0?this.urlHandlingStrategy.merge(t,i):i,s=r??o;return s instanceof zi?this.urlSerializer.serialize(s):s}commitTransition({targetRouterState:t,finalUrl:i,initialUrl:r}){i&&t?(this.currentUrlTree=i,this.rawUrlTree=this.urlHandlingStrategy.merge(i,r),this.routerState=t):this.rawUrlTree=r}routerState=KM(null,ce(rn));getRouterState(){return this.routerState}_stateMemento=this.createStateMemento();get stateMemento(){return this._stateMemento}updateStateMemento(){this._stateMemento=this.createStateMemento()}createStateMemento(){return{rawUrlTree:this.rawUrlTree,currentUrlTree:this.currentUrlTree,routerState:this.routerState}}restoredState(){return this.location.getState()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:()=>ce(M1),providedIn:"root"})}return n})(),M1=(()=>{class n extends qv{currentPageId=0;lastSuccessfulId=-1;get browserPageId(){return this.canceledNavigationResolution!=="computed"?this.currentPageId:this.restoredState()?.\u0275routerPageId??this.currentPageId}registerNonRouterCurrentEntryChangeListener(t){return this.location.subscribe(i=>{i.type==="popstate"&&setTimeout(()=>{t(i.url,i.state,"popstate",{replaceUrl:!0})})})}handleRouterEvent(t,i){t instanceof ma?this.updateStateMemento():t instanceof uo?this.commitTransition(i):t instanceof Qd?this.urlUpdateStrategy==="eager"&&(i.extras.skipLocationChange||this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof va?(this.commitTransition(i),this.urlUpdateStrategy==="deferred"&&!i.extras.skipLocationChange&&this.setBrowserUrl(this.createBrowserPath(i),i)):t instanceof Vi&&!ZM(t)?this.restoreHistory(i):t instanceof ga?this.restoreHistory(i,!0):t instanceof lo&&(this.lastSuccessfulId=t.id,this.currentPageId=this.browserPageId)}setBrowserUrl(t,{extras:i,id:r}){let{replaceUrl:o,state:s}=i;if(this.location.isCurrentPathEqualTo(t)||o){let a=this.browserPageId,c=Ee(Ee({},s),this.generateNgRouterState(r,a));this.location.replaceState(t,"",c)}else{let a=Ee(Ee({},s),this.generateNgRouterState(r,this.browserPageId+1));this.location.go(t,"",a)}}restoreHistory(t,i=!1){if(this.canceledNavigationResolution==="computed"){let r=this.browserPageId,o=this.currentPageId-r;o!==0?this.location.historyGo(o):this.getCurrentUrlTree()===t.finalUrl&&o===0&&(this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}else this.canceledNavigationResolution==="replace"&&(i&&this.resetInternalState(t),this.resetUrlToCurrentUrlTree())}resetInternalState({finalUrl:t}){this.routerState=this.stateMemento.routerState,this.currentUrlTree=this.stateMemento.currentUrlTree,this.rawUrlTree=this.urlHandlingStrategy.merge(this.currentUrlTree,t??this.rawUrlTree)}resetUrlToCurrentUrlTree(){this.location.replaceState(this.urlSerializer.serialize(this.getRawUrlTree()),"",this.generateNgRouterState(this.lastSuccessfulId,this.currentPageId))}generateNgRouterState(t,i){return this.canceledNavigationResolution==="computed"?{navigationId:t,\u0275routerPageId:i}:{navigationId:t}}static \u0275fac=(()=>{let t;return function(r){return(t||(t=Hc(n)))(r||n)}})();static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Eb(n,e){n.events.pipe(xr(t=>t instanceof lo||t instanceof Vi||t instanceof ga||t instanceof uo),qt(t=>t instanceof lo||t instanceof uo?0:(t instanceof Vi?t.code===Hn.Redirect||t.code===Hn.SupersededByNewNavigation:!1)?2:1),xr(t=>t!==2),Er(1)).subscribe(()=>{e()})}var Xv=(()=>{class n{get currentUrlTree(){return this.stateManager.getCurrentUrlTree()}get rawUrlTree(){return this.stateManager.getRawUrlTree()}disposed=!1;nonRouterCurrentEntryChangeSubscription;console=ce(qg);stateManager=ce(qv);options=ce(lf,{optional:!0})||{};pendingTasks=ce(to);urlUpdateStrategy=this.options.urlUpdateStrategy||"deferred";navigationTransitions=ce(_b);urlSerializer=ce(sf);location=ce(la);urlHandlingStrategy=ce($v);injector=ce(rn);_events=new Mn;get events(){return this._events}get routerState(){return this.stateManager.getRouterState()}navigated=!1;routeReuseStrategy=ce(x1);injectorCleanup=ce(xb,{optional:!0});onSameUrlNavigation=this.options.onSameUrlNavigation||"ignore";config=ce(uf,{optional:!0})?.flat()??[];componentInputBindingEnabled=!!ce(af,{optional:!0});currentNavigation=this.navigationTransitions.currentNavigation.asReadonly();constructor(){this.resetConfig(this.config),this.navigationTransitions.setupNavigations(this).subscribe({error:t=>{}}),this.subscribeToNavigationEvents()}eventsSubscription=new En;subscribeToNavigationEvents(){let t=this.navigationTransitions.events.subscribe(i=>{try{let r=this.navigationTransitions.currentTransition,o=so(this.navigationTransitions.currentNavigation);if(r!==null&&o!==null){if(this.stateManager.handleRouterEvent(i,o),i instanceof Vi&&i.code!==Hn.Redirect&&i.code!==Hn.SupersededByNewNavigation)this.navigated=!0;else if(i instanceof lo)this.navigated=!0,this.injectorCleanup?.(this.routeReuseStrategy,this.routerState,this.config);else if(i instanceof ya){let s=i.navigationBehaviorOptions,a=this.urlHandlingStrategy.merge(i.url,r.currentRawUrl),c=Ee({scroll:r.extras.scroll,browserUrl:r.extras.browserUrl,info:r.extras.info,skipLocationChange:r.extras.skipLocationChange,replaceUrl:r.extras.replaceUrl||this.urlUpdateStrategy==="eager"||_1(r.source)},s);this.scheduleNavigation(a,ol,null,c,{resolve:r.resolve,reject:r.reject,promise:r.promise})}}yD(i)&&this._events.next(i)}catch(r){this.navigationTransitions.transitionAbortWithErrorSubject.next(r)}});this.eventsSubscription.add(t)}resetRootComponentType(t){this.routerState.root.component=t,this.navigationTransitions.rootComponentType=t}initialNavigation(){this.setUpLocationChangeListener(),this.navigationTransitions.hasRequestedNavigation||this.navigateToSyncWithBrowser(this.location.path(!0),ol,this.stateManager.restoredState(),{replaceUrl:!0})}setUpLocationChangeListener(){this.nonRouterCurrentEntryChangeSubscription??=this.stateManager.registerNonRouterCurrentEntryChangeListener((t,i,r,o)=>{this.navigateToSyncWithBrowser(t,r,i,o)})}navigateToSyncWithBrowser(t,i,r,o){let s=r?.navigationId?r:null;if(r){let c=Ee({},r);delete c.navigationId,delete c.\u0275routerPageId,Object.keys(c).length!==0&&(o.state=c)}let a=this.parseUrl(t);this.scheduleNavigation(a,i,s,o).catch(c=>{this.disposed||this.injector.get(ir)(c)})}get url(){return this.serializeUrl(this.currentUrlTree)}getCurrentNavigation(){return so(this.navigationTransitions.currentNavigation)}get lastSuccessfulNavigation(){return this.navigationTransitions.lastSuccessfulNavigation}resetConfig(t){this.config=t.map(jv),this.navigated=!1}ngOnDestroy(){this.dispose()}dispose(){this._events.unsubscribe(),this.navigationTransitions.complete(),this.nonRouterCurrentEntryChangeSubscription?.unsubscribe(),this.nonRouterCurrentEntryChangeSubscription=void 0,this.disposed=!0,this.eventsSubscription.unsubscribe()}createUrlTree(t,i={}){let{relativeTo:r,queryParams:o,fragment:s,queryParamsHandling:a,preserveFragment:c}=i,l=c?this.currentUrlTree.fragment:s,u=null;switch(a??this.options.defaultQueryParamsHandling){case"merge":u=Ee(Ee({},this.currentUrlTree.queryParams),o);break;case"preserve":u=this.currentUrlTree.queryParams;break;default:u=o||null}u!==null&&(u=this.removeEmptyProps(u));let d;try{let f=r?r.snapshot:this.routerState.snapshot.root;d=$M(f)}catch{(typeof t[0]!="string"||t[0][0]!=="/")&&(t=[]),d=this.currentUrlTree.root}return qM(d,t,u,l??null,this.urlSerializer)}navigateByUrl(t,i={skipLocationChange:!1}){let r=pa(t)?t:this.parseUrl(t),o=this.urlHandlingStrategy.merge(r,this.rawUrlTree);return this.scheduleNavigation(o,ol,null,i)}navigate(t,i={skipLocationChange:!1}){return b1(t),this.navigateByUrl(this.createUrlTree(t,i),i)}serializeUrl(t){return this.urlSerializer.serialize(t)}parseUrl(t){try{return this.urlSerializer.parse(t)}catch{return this.console.warn(Ec(4018,!1)),this.urlSerializer.parse("/")}}isActive(t,i){let r;if(i===!0?r=Ee({},UM):i===!1?r=Ee({},yv):r=Ee(Ee({},yv),i),pa(t))return IM(this.currentUrlTree,t,r);let o=this.parseUrl(t);return IM(this.currentUrlTree,o,r)}removeEmptyProps(t){return Object.entries(t).reduce((i,[r,o])=>(o!=null&&(i[r]=o),i),{})}scheduleNavigation(t,i,r,o,s){if(this.disposed)return Promise.resolve(!1);let a,c,l;s?(a=s.resolve,c=s.reject,l=s.promise):l=new Promise((d,f)=>{a=d,c=f});let u=this.pendingTasks.add();return Eb(this,()=>{queueMicrotask(()=>this.pendingTasks.remove(u))}),this.navigationTransitions.handleNavigationRequest({source:i,restoredState:r,currentUrlTree:this.currentUrlTree,currentRawUrl:this.currentUrlTree,rawUrl:t,extras:o,resolve:a,reject:c,promise:l,currentSnapshot:this.routerState.snapshot,currentRouterState:this.routerState}),l.catch(Promise.reject.bind(Promise))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=je({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function b1(n){for(let e=0;e<n.length;e++)if(n[e]==null)throw new Oe(4008,!1)}var S1=new Ve("");function Yv(n,...e){return Vs([{provide:uf,multi:!0,useValue:n},[],{provide:ss,useFactory:w1},{provide:Dd,multi:!0,useFactory:T1},e.map(t=>t.\u0275providers)])}function w1(){return ce(Xv).routerState.root}function T1(){let n=ce(Ji);return e=>{let t=n.get(sa);if(e!==t.components[0])return;let i=n.get(Xv),r=n.get(C1);n.get(I1)===1&&i.initialNavigation(),n.get(A1,null,{optional:!0})?.setUpPreloading(),n.get(S1,null,{optional:!0})?.init(),i.resetRootComponentType(t.componentTypes[0]),r.closed||(r.next(),r.complete(),r.unsubscribe())}}var C1=new Ve("",{factory:()=>new Mn}),I1=new Ve("",{factory:()=>1});var A1=new Ve("");var Mb=[];var bb={providers:[qm(),Yv(Mb)]};var Co={LEFT:0,MIDDLE:1,RIGHT:2,ROTATE:0,DOLLY:1,PAN:2},Io={ROTATE:0,PAN:1,DOLLY_PAN:2,DOLLY_ROTATE:3},Xb=0,Ry=1,Yb=2;var Vl=1,dh=2,qa=3,Or=0,zn=1,Jt=2,hr=0,hs=1,Dy=2,Ny=3,Py=4,Zb=5;var _o=100,Kb=101,Jb=102,Qb=103,eS=104,tS=200,nS=201,iS=202,rS=203,Lf=204,Of=205,oS=206,sS=207,aS=208,cS=209,lS=210,uS=211,dS=212,fS=213,hS=214,Ff=0,kf=1,Uf=2,ps=3,Bf=4,Hf=5,Vf=6,zf=7,Ly=0,pS=1,mS=2,qi=0,Oy=1,Fy=2,ky=3,zl=4,Uy=5,By=6,Hy=7;var _y=300,Ao=301,xs=302,fh=303,hh=304,Gl=306,li=1e3,lr=1001,Gf=1002,yn=1003,gS=1004;var Wl=1005;var Cn=1006,ph=1007;var Ro=1008;var Yn=1009,Vy=1010,zy=1011,Xa=1012,mh=1013,Xi=1014,Yi=1015,pr=1016,gh=1017,vh=1018,Ya=1020,Gy=35902,Wy=35899,jy=1021,$y=1022,Ti=1023,ur=1026,Do=1027,qy=1028,yh=1029,Es=1030,_h=1031;var xh=1033,jl=33776,$l=33777,ql=33778,Xl=33779,Eh=35840,Mh=35841,bh=35842,Sh=35843,wh=36196,Th=37492,Ch=37496,Ih=37488,Ah=37489,Rh=37490,Dh=37491,Nh=37808,Ph=37809,Lh=37810,Oh=37811,Fh=37812,kh=37813,Uh=37814,Bh=37815,Hh=37816,Vh=37817,zh=37818,Gh=37819,Wh=37820,jh=37821,$h=36492,qh=36494,Xh=36495,Yh=36283,Zh=36284,Kh=36285,Jh=36286;var bl=2300,Wf=2301,Pf=2302,xy=2303,Ey=2400,My=2401,by=2402;var vS=3200;var Xy=0,yS=1,kr="",ai="srgb",ms="srgb-linear",Sl="linear",At="srgb";var fs=7680;var Sy=519,_S=512,xS=513,ES=514,Qh=515,MS=516,bS=517,ep=518,SS=519,wy=35044;var Yy="300 es",$i=2e3,Oa=2001;function D1(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function N1(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function wl(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function wS(){let n=wl("canvas");return n.style.display="block",n}var Sb={},Fa=null;function Zy(...n){let e="THREE."+n.shift();Fa?Fa("log",e,...n):console.log(e,...n)}function TS(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function Fe(...n){n=TS(n);let e="THREE."+n.shift();if(Fa)Fa("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function ke(...n){n=TS(n);let e="THREE."+n.shift();if(Fa)Fa("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function Tl(...n){let e=n.join(" ");e in Sb||(Sb[e]=!0,Fe(...n))}function CS(n,e,t){return new Promise(function(i,r){function o(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(o,t);break;default:i()}}setTimeout(o,t)})}var IS={[Ff]:kf,[Uf]:Vf,[Bf]:zf,[ps]:Hf,[kf]:Ff,[Vf]:Uf,[zf]:Bf,[Hf]:ps},dr=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let o=r.indexOf(t);o!==-1&&r.splice(o,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let o=0,s=r.length;o<s;o++)r[o].call(this,e);e.target=null}}},Pn=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"],wb=1234567,El=Math.PI/180,ka=180/Math.PI;function Za(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(Pn[n&255]+Pn[n>>8&255]+Pn[n>>16&255]+Pn[n>>24&255]+"-"+Pn[e&255]+Pn[e>>8&255]+"-"+Pn[e>>16&15|64]+Pn[e>>24&255]+"-"+Pn[t&63|128]+Pn[t>>8&255]+"-"+Pn[t>>16&255]+Pn[t>>24&255]+Pn[i&255]+Pn[i>>8&255]+Pn[i>>16&255]+Pn[i>>24&255]).toLowerCase()}function ot(n,e,t){return Math.max(e,Math.min(t,n))}function Ky(n,e){return(n%e+e)%e}function P1(n,e,t,i,r){return i+(n-e)*(r-i)/(t-e)}function L1(n,e,t){return n!==e?(t-n)/(e-n):0}function Ml(n,e,t){return(1-t)*n+t*e}function O1(n,e,t,i){return Ml(n,e,1-Math.exp(-t*i))}function F1(n,e=1){return e-Math.abs(Ky(n,e*2)-e)}function k1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*(3-2*n))}function U1(n,e,t){return n<=e?0:n>=t?1:(n=(n-e)/(t-e),n*n*n*(n*(n*6-15)+10))}function B1(n,e){return n+Math.floor(Math.random()*(e-n+1))}function H1(n,e){return n+Math.random()*(e-n)}function V1(n){return n*(.5-Math.random())}function z1(n){n!==void 0&&(wb=n);let e=wb+=1831565813;return e=Math.imul(e^e>>>15,e|1),e^=e+Math.imul(e^e>>>7,e|61),((e^e>>>14)>>>0)/4294967296}function G1(n){return n*El}function W1(n){return n*ka}function j1(n){return(n&n-1)===0&&n!==0}function $1(n){return Math.pow(2,Math.ceil(Math.log(n)/Math.LN2))}function q1(n){return Math.pow(2,Math.floor(Math.log(n)/Math.LN2))}function X1(n,e,t,i,r){let o=Math.cos,s=Math.sin,a=o(t/2),c=s(t/2),l=o((e+i)/2),u=s((e+i)/2),d=o((e-i)/2),f=s((e-i)/2),h=o((i-e)/2),m=s((i-e)/2);switch(r){case"XYX":n.set(a*u,c*d,c*f,a*l);break;case"YZY":n.set(c*f,a*u,c*d,a*l);break;case"ZXZ":n.set(c*d,c*f,a*u,a*l);break;case"XZX":n.set(a*u,c*m,c*h,a*l);break;case"YXY":n.set(c*h,a*u,c*m,a*l);break;case"ZYZ":n.set(c*m,c*h,a*u,a*l);break;default:Fe("MathUtils: .setQuaternionFromProperEuler() encountered an unknown order: "+r)}}function Pa(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function Vn(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var Jy={DEG2RAD:El,RAD2DEG:ka,generateUUID:Za,clamp:ot,euclideanModulo:Ky,mapLinear:P1,inverseLerp:L1,lerp:Ml,damp:O1,pingpong:F1,smoothstep:k1,smootherstep:U1,randInt:B1,randFloat:H1,randFloatSpread:V1,seededRandom:z1,degToRad:G1,radToDeg:W1,isPowerOfTwo:j1,ceilPowerOfTwo:$1,floorPowerOfTwo:q1,setQuaternionFromProperEuler:X1,normalize:Vn,denormalize:Pa},Ue=class n{constructor(e=0,t=0){n.prototype.isVector2=!0,this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=ot(this.x,e.x,t.x),this.y=ot(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=ot(this.x,e,t),this.y=ot(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(ot(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(ot(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),o=this.x-e.x,s=this.y-e.y;return this.x=o*i-s*r+e.x,this.y=o*r+s*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},ui=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,o,s,a){let c=i[r+0],l=i[r+1],u=i[r+2],d=i[r+3],f=o[s+0],h=o[s+1],m=o[s+2],y=o[s+3];if(d!==y||c!==f||l!==h||u!==m){let g=c*f+l*h+u*m+d*y;g<0&&(f=-f,h=-h,m=-m,y=-y,g=-g);let p=1-a;if(g<.9995){let E=Math.acos(g),w=Math.sin(E);p=Math.sin(p*E)/w,a=Math.sin(a*E)/w,c=c*p+f*a,l=l*p+h*a,u=u*p+m*a,d=d*p+y*a}else{c=c*p+f*a,l=l*p+h*a,u=u*p+m*a,d=d*p+y*a;let E=1/Math.sqrt(c*c+l*l+u*u+d*d);c*=E,l*=E,u*=E,d*=E}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=d}static multiplyQuaternionsFlat(e,t,i,r,o,s){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],d=o[s],f=o[s+1],h=o[s+2],m=o[s+3];return e[t]=a*m+u*d+c*h-l*f,e[t+1]=c*m+u*f+l*d-a*h,e[t+2]=l*m+u*h+a*f-c*d,e[t+3]=u*m-a*d-c*f-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,o=e._z,s=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),d=a(o/2),f=c(i/2),h=c(r/2),m=c(o/2);switch(s){case"XYZ":this._x=f*u*d+l*h*m,this._y=l*h*d-f*u*m,this._z=l*u*m+f*h*d,this._w=l*u*d-f*h*m;break;case"YXZ":this._x=f*u*d+l*h*m,this._y=l*h*d-f*u*m,this._z=l*u*m-f*h*d,this._w=l*u*d+f*h*m;break;case"ZXY":this._x=f*u*d-l*h*m,this._y=l*h*d+f*u*m,this._z=l*u*m+f*h*d,this._w=l*u*d-f*h*m;break;case"ZYX":this._x=f*u*d-l*h*m,this._y=l*h*d+f*u*m,this._z=l*u*m-f*h*d,this._w=l*u*d+f*h*m;break;case"YZX":this._x=f*u*d+l*h*m,this._y=l*h*d+f*u*m,this._z=l*u*m-f*h*d,this._w=l*u*d-f*h*m;break;case"XZY":this._x=f*u*d-l*h*m,this._y=l*h*d-f*u*m,this._z=l*u*m+f*h*d,this._w=l*u*d+f*h*m;break;default:Fe("Quaternion: .setFromEuler() encountered an unknown order: "+s)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],o=t[8],s=t[1],a=t[5],c=t[9],l=t[2],u=t[6],d=t[10],f=i+a+d;if(f>0){let h=.5/Math.sqrt(f+1);this._w=.25/h,this._x=(u-c)*h,this._y=(o-l)*h,this._z=(s-r)*h}else if(i>a&&i>d){let h=2*Math.sqrt(1+i-a-d);this._w=(u-c)/h,this._x=.25*h,this._y=(r+s)/h,this._z=(o+l)/h}else if(a>d){let h=2*Math.sqrt(1+a-i-d);this._w=(o-l)/h,this._x=(r+s)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+d-i-a);this._w=(s-r)/h,this._x=(o+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(ot(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,o=e._z,s=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+s*a+r*l-o*c,this._y=r*u+s*c+o*a-i*l,this._z=o*u+s*l+i*c-r*a,this._w=s*u-i*a-r*c-o*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,o=e._z,s=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,o=-o,s=-s,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+o*t,this._w=this._w*c+s*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+o*t,this._w=this._w*c+s*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),o=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),o*Math.sin(t),o*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},T=class n{constructor(e=0,t=0,i=0){n.prototype.isVector3=!0,this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(Tb.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(Tb.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,o=e.elements;return this.x=o[0]*t+o[3]*i+o[6]*r,this.y=o[1]*t+o[4]*i+o[7]*r,this.z=o[2]*t+o[5]*i+o[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,o=e.elements,s=1/(o[3]*t+o[7]*i+o[11]*r+o[15]);return this.x=(o[0]*t+o[4]*i+o[8]*r+o[12])*s,this.y=(o[1]*t+o[5]*i+o[9]*r+o[13])*s,this.z=(o[2]*t+o[6]*i+o[10]*r+o[14])*s,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,o=e.x,s=e.y,a=e.z,c=e.w,l=2*(s*r-a*i),u=2*(a*t-o*r),d=2*(o*i-s*t);return this.x=t+c*l+s*d-a*u,this.y=i+c*u+a*l-o*d,this.z=r+c*d+o*u-s*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r,this.y=o[1]*t+o[5]*i+o[9]*r,this.z=o[2]*t+o[6]*i+o[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=ot(this.x,e.x,t.x),this.y=ot(this.y,e.y,t.y),this.z=ot(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=ot(this.x,e,t),this.y=ot(this.y,e,t),this.z=ot(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(ot(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,o=e.z,s=t.x,a=t.y,c=t.z;return this.x=r*c-o*a,this.y=o*s-i*c,this.z=i*a-r*s,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return Zv.copy(this).projectOnVector(e),this.sub(Zv)}reflect(e){return this.sub(Zv.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(ot(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},Zv=new T,Tb=new ui,et=class n{constructor(e,t,i,r,o,s,a,c,l){n.prototype.isMatrix3=!0,this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,o,s,a,c,l)}set(e,t,i,r,o,s,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=o,u[5]=c,u[6]=i,u[7]=s,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,o=this.elements,s=i[0],a=i[3],c=i[6],l=i[1],u=i[4],d=i[7],f=i[2],h=i[5],m=i[8],y=r[0],g=r[3],p=r[6],E=r[1],w=r[4],S=r[7],I=r[2],R=r[5],D=r[8];return o[0]=s*y+a*E+c*I,o[3]=s*g+a*w+c*R,o[6]=s*p+a*S+c*D,o[1]=l*y+u*E+d*I,o[4]=l*g+u*w+d*R,o[7]=l*p+u*S+d*D,o[2]=f*y+h*E+m*I,o[5]=f*g+h*w+m*R,o[8]=f*p+h*S+m*D,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],o=e[3],s=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*s*u-t*a*l-i*o*u+i*a*c+r*o*l-r*s*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],o=e[3],s=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=u*s-a*l,f=a*c-u*o,h=l*o-s*c,m=t*d+i*f+r*h;if(m===0)return this.set(0,0,0,0,0,0,0,0,0);let y=1/m;return e[0]=d*y,e[1]=(r*l-u*i)*y,e[2]=(a*i-r*s)*y,e[3]=f*y,e[4]=(u*t-r*c)*y,e[5]=(r*o-a*t)*y,e[6]=h*y,e[7]=(i*c-l*t)*y,e[8]=(s*t-i*o)*y,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,o,s,a){let c=Math.cos(o),l=Math.sin(o);return this.set(i*c,i*l,-i*(c*s+l*a)+s+e,-r*l,r*c,-r*(-l*s+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(Kv.makeScale(e,t)),this}rotate(e){return this.premultiply(Kv.makeRotation(-e)),this}translate(e,t){return this.premultiply(Kv.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},Kv=new et,Cb=new et().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),Ib=new et().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function Y1(){let n={enabled:!0,workingColorSpace:ms,spaces:{},convert:function(r,o,s){return this.enabled===!1||o===s||!o||!s||(this.spaces[o].transfer===At&&(r.r=Lr(r.r),r.g=Lr(r.g),r.b=Lr(r.b)),this.spaces[o].primaries!==this.spaces[s].primaries&&(r.applyMatrix3(this.spaces[o].toXYZ),r.applyMatrix3(this.spaces[s].fromXYZ)),this.spaces[s].transfer===At&&(r.r=La(r.r),r.g=La(r.g),r.b=La(r.b))),r},workingToColorSpace:function(r,o){return this.convert(r,this.workingColorSpace,o)},colorSpaceToWorking:function(r,o){return this.convert(r,o,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===kr?Sl:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,o=this.workingColorSpace){return r.fromArray(this.spaces[o].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,o,s){return r.copy(this.spaces[o].toXYZ).multiply(this.spaces[s].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,o){return Tl("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,o)},toWorkingColorSpace:function(r,o){return Tl("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,o)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[ms]:{primaries:e,whitePoint:i,transfer:Sl,toXYZ:Cb,fromXYZ:Ib,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:ai},outputColorSpaceConfig:{drawingBufferColorSpace:ai}},[ai]:{primaries:e,whitePoint:i,transfer:At,toXYZ:Cb,fromXYZ:Ib,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:ai}}}),n}var mt=Y1();function Lr(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function La(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var Ea,jf=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{Ea===void 0&&(Ea=wl("canvas")),Ea.width=e.width,Ea.height=e.height;let r=Ea.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=Ea}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=wl("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),o=r.data;for(let s=0;s<o.length;s++)o[s]=Lr(o[s]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(Lr(t[i]/255)*255):t[i]=Lr(t[i]);return{data:t,width:e.width,height:e.height}}else return Fe("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},Z1=0,Ua=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:Z1++}),this.uuid=Za(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayHeight,t.displayWidth,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let o;if(Array.isArray(r)){o=[];for(let s=0,a=r.length;s<a;s++)r[s].isDataTexture?o.push(Jv(r[s].image)):o.push(Jv(r[s]))}else o=Jv(r);i.url=o}return t||(e.images[this.uuid]=i),i}};function Jv(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?jf.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(Fe("Texture: Unable to serialize Texture."),{})}var K1=0,Qv=new T,mr=(()=>{class n extends dr{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=lr,o=lr,s=Cn,a=Ro,c=Ti,l=Yn,u=n.DEFAULT_ANISOTROPY,d=kr){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:K1++}),this.uuid=Za(),this.name="",this.source=new Ua(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=o,this.magFilter=s,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new Ue(0,0),this.repeat=new Ue(1,1),this.center=new Ue(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new et,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=d,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0}get width(){return this.source.getSize(Qv).x}get height(){return this.source.getSize(Qv).y}get depth(){return this.source.getSize(Qv).z}get image(){return this.source.data}set image(t=null){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){Fe(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let o=this[i];if(o===void 0){Fe(`Texture.setValues(): property '${i}' does not exist.`);continue}o&&r&&o.isVector2&&r.isVector2||o&&r&&o.isVector3&&r.isVector3||o&&r&&o.isMatrix3&&r.isMatrix3?o.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==_y)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case li:t.x=t.x-Math.floor(t.x);break;case lr:t.x=t.x<0?0:1;break;case Gf:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case li:t.y=t.y-Math.floor(t.y);break;case lr:t.y=t.y<0?0:1;break;case Gf:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=_y,n.DEFAULT_ANISOTROPY=1,n})(),jt=class n{constructor(e=0,t=0,i=0,r=1){n.prototype.isVector4=!0,this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,o=this.w,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r+s[12]*o,this.y=s[1]*t+s[5]*i+s[9]*r+s[13]*o,this.z=s[2]*t+s[6]*i+s[10]*r+s[14]*o,this.w=s[3]*t+s[7]*i+s[11]*r+s[15]*o,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,o,c=e.elements,l=c[0],u=c[4],d=c[8],f=c[1],h=c[5],m=c[9],y=c[2],g=c[6],p=c[10];if(Math.abs(u-f)<.01&&Math.abs(d-y)<.01&&Math.abs(m-g)<.01){if(Math.abs(u+f)<.1&&Math.abs(d+y)<.1&&Math.abs(m+g)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let w=(l+1)/2,S=(h+1)/2,I=(p+1)/2,R=(u+f)/4,D=(d+y)/4,_=(m+g)/4;return w>S&&w>I?w<.01?(i=0,r=.707106781,o=.707106781):(i=Math.sqrt(w),r=R/i,o=D/i):S>I?S<.01?(i=.707106781,r=0,o=.707106781):(r=Math.sqrt(S),i=R/r,o=_/r):I<.01?(i=.707106781,r=.707106781,o=0):(o=Math.sqrt(I),i=D/o,r=_/o),this.set(i,r,o,t),this}let E=Math.sqrt((g-m)*(g-m)+(d-y)*(d-y)+(f-u)*(f-u));return Math.abs(E)<.001&&(E=1),this.x=(g-m)/E,this.y=(d-y)/E,this.z=(f-u)/E,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=ot(this.x,e.x,t.x),this.y=ot(this.y,e.y,t.y),this.z=ot(this.z,e.z,t.z),this.w=ot(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=ot(this.x,e,t),this.y=ot(this.y,e,t),this.z=ot(this.z,e,t),this.w=ot(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(ot(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},$f=class extends dr{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:Cn,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new jt(0,0,e,t),this.scissorTest=!1,this.viewport=new jt(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},o=new mr(r),s=i.count;for(let a=0;a<s;a++)this.textures[a]=o.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:Cn,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,o=this.textures.length;r<o;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new Ua(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this}dispose(){this.dispatchEvent({type:"dispose"})}},di=class extends $f{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Cl=class extends mr{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=yn,this.minFilter=yn,this.wrapR=lr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var qf=class extends mr{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=yn,this.minFilter=yn,this.wrapR=lr,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Bt=class n{constructor(e,t,i,r,o,s,a,c,l,u,d,f,h,m,y,g){n.prototype.isMatrix4=!0,this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,o,s,a,c,l,u,d,f,h,m,y,g)}set(e,t,i,r,o,s,a,c,l,u,d,f,h,m,y,g){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=o,p[5]=s,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=d,p[14]=f,p[3]=h,p[7]=m,p[11]=y,p[15]=g,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/Ma.setFromMatrixColumn(e,0).length(),o=1/Ma.setFromMatrixColumn(e,1).length(),s=1/Ma.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*o,t[5]=i[5]*o,t[6]=i[6]*o,t[7]=0,t[8]=i[8]*s,t[9]=i[9]*s,t[10]=i[10]*s,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,o=e.z,s=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(o),d=Math.sin(o);if(e.order==="XYZ"){let f=s*u,h=s*d,m=a*u,y=a*d;t[0]=c*u,t[4]=-c*d,t[8]=l,t[1]=h+m*l,t[5]=f-y*l,t[9]=-a*c,t[2]=y-f*l,t[6]=m+h*l,t[10]=s*c}else if(e.order==="YXZ"){let f=c*u,h=c*d,m=l*u,y=l*d;t[0]=f+y*a,t[4]=m*a-h,t[8]=s*l,t[1]=s*d,t[5]=s*u,t[9]=-a,t[2]=h*a-m,t[6]=y+f*a,t[10]=s*c}else if(e.order==="ZXY"){let f=c*u,h=c*d,m=l*u,y=l*d;t[0]=f-y*a,t[4]=-s*d,t[8]=m+h*a,t[1]=h+m*a,t[5]=s*u,t[9]=y-f*a,t[2]=-s*l,t[6]=a,t[10]=s*c}else if(e.order==="ZYX"){let f=s*u,h=s*d,m=a*u,y=a*d;t[0]=c*u,t[4]=m*l-h,t[8]=f*l+y,t[1]=c*d,t[5]=y*l+f,t[9]=h*l-m,t[2]=-l,t[6]=a*c,t[10]=s*c}else if(e.order==="YZX"){let f=s*c,h=s*l,m=a*c,y=a*l;t[0]=c*u,t[4]=y-f*d,t[8]=m*d+h,t[1]=d,t[5]=s*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*d+m,t[10]=f-y*d}else if(e.order==="XZY"){let f=s*c,h=s*l,m=a*c,y=a*l;t[0]=c*u,t[4]=-d,t[8]=l*u,t[1]=f*d+y,t[5]=s*u,t[9]=h*d-m,t[2]=m*d-h,t[6]=a*u,t[10]=y*d+f}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(J1,e,Q1)}lookAt(e,t,i){let r=this.elements;return oi.subVectors(e,t),oi.lengthSq()===0&&(oi.z=1),oi.normalize(),fo.crossVectors(i,oi),fo.lengthSq()===0&&(Math.abs(i.z)===1?oi.x+=1e-4:oi.z+=1e-4,oi.normalize(),fo.crossVectors(i,oi)),fo.normalize(),df.crossVectors(oi,fo),r[0]=fo.x,r[4]=df.x,r[8]=oi.x,r[1]=fo.y,r[5]=df.y,r[9]=oi.y,r[2]=fo.z,r[6]=df.z,r[10]=oi.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,o=this.elements,s=i[0],a=i[4],c=i[8],l=i[12],u=i[1],d=i[5],f=i[9],h=i[13],m=i[2],y=i[6],g=i[10],p=i[14],E=i[3],w=i[7],S=i[11],I=i[15],R=r[0],D=r[4],_=r[8],M=r[12],q=r[1],C=r[5],U=r[9],B=r[13],W=r[2],H=r[6],V=r[10],O=r[14],ee=r[3],Z=r[7],fe=r[11],ye=r[15];return o[0]=s*R+a*q+c*W+l*ee,o[4]=s*D+a*C+c*H+l*Z,o[8]=s*_+a*U+c*V+l*fe,o[12]=s*M+a*B+c*O+l*ye,o[1]=u*R+d*q+f*W+h*ee,o[5]=u*D+d*C+f*H+h*Z,o[9]=u*_+d*U+f*V+h*fe,o[13]=u*M+d*B+f*O+h*ye,o[2]=m*R+y*q+g*W+p*ee,o[6]=m*D+y*C+g*H+p*Z,o[10]=m*_+y*U+g*V+p*fe,o[14]=m*M+y*B+g*O+p*ye,o[3]=E*R+w*q+S*W+I*ee,o[7]=E*D+w*C+S*H+I*Z,o[11]=E*_+w*U+S*V+I*fe,o[15]=E*M+w*B+S*O+I*ye,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],o=e[12],s=e[1],a=e[5],c=e[9],l=e[13],u=e[2],d=e[6],f=e[10],h=e[14],m=e[3],y=e[7],g=e[11],p=e[15],E=c*h-l*f,w=a*h-l*d,S=a*f-c*d,I=s*h-l*u,R=s*f-c*u,D=s*d-a*u;return t*(y*E-g*w+p*S)-i*(m*E-g*I+p*R)+r*(m*w-y*I+p*D)-o*(m*S-y*R+g*D)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],o=e[3],s=e[4],a=e[5],c=e[6],l=e[7],u=e[8],d=e[9],f=e[10],h=e[11],m=e[12],y=e[13],g=e[14],p=e[15],E=t*a-i*s,w=t*c-r*s,S=t*l-o*s,I=i*c-r*a,R=i*l-o*a,D=r*l-o*c,_=u*y-d*m,M=u*g-f*m,q=u*p-h*m,C=d*g-f*y,U=d*p-h*y,B=f*p-h*g,W=E*B-w*U+S*C+I*q-R*M+D*_;if(W===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let H=1/W;return e[0]=(a*B-c*U+l*C)*H,e[1]=(r*U-i*B-o*C)*H,e[2]=(y*D-g*R+p*I)*H,e[3]=(f*R-d*D-h*I)*H,e[4]=(c*q-s*B-l*M)*H,e[5]=(t*B-r*q+o*M)*H,e[6]=(g*S-m*D-p*w)*H,e[7]=(u*D-f*S+h*w)*H,e[8]=(s*U-a*q+l*_)*H,e[9]=(i*q-t*U-o*_)*H,e[10]=(m*R-y*S+p*E)*H,e[11]=(d*S-u*R-h*E)*H,e[12]=(a*M-s*C-c*_)*H,e[13]=(t*C-i*M+r*_)*H,e[14]=(y*w-m*I-g*E)*H,e[15]=(u*I-d*w+f*E)*H,this}scale(e){let t=this.elements,i=e.x,r=e.y,o=e.z;return t[0]*=i,t[4]*=r,t[8]*=o,t[1]*=i,t[5]*=r,t[9]*=o,t[2]*=i,t[6]*=r,t[10]*=o,t[3]*=i,t[7]*=r,t[11]*=o,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),o=1-i,s=e.x,a=e.y,c=e.z,l=o*s,u=o*a;return this.set(l*s+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*s,0,l*c-r*a,u*c+r*s,o*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,o,s){return this.set(1,i,o,0,e,1,s,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,o=t._x,s=t._y,a=t._z,c=t._w,l=o+o,u=s+s,d=a+a,f=o*l,h=o*u,m=o*d,y=s*u,g=s*d,p=a*d,E=c*l,w=c*u,S=c*d,I=i.x,R=i.y,D=i.z;return r[0]=(1-(y+p))*I,r[1]=(h+S)*I,r[2]=(m-w)*I,r[3]=0,r[4]=(h-S)*R,r[5]=(1-(f+p))*R,r[6]=(g+E)*R,r[7]=0,r[8]=(m+w)*D,r[9]=(g-E)*D,r[10]=(1-(f+y))*D,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let o=this.determinant();if(o===0)return i.set(1,1,1),t.identity(),this;let s=Ma.set(r[0],r[1],r[2]).length(),a=Ma.set(r[4],r[5],r[6]).length(),c=Ma.set(r[8],r[9],r[10]).length();o<0&&(s=-s),Gi.copy(this);let l=1/s,u=1/a,d=1/c;return Gi.elements[0]*=l,Gi.elements[1]*=l,Gi.elements[2]*=l,Gi.elements[4]*=u,Gi.elements[5]*=u,Gi.elements[6]*=u,Gi.elements[8]*=d,Gi.elements[9]*=d,Gi.elements[10]*=d,t.setFromRotationMatrix(Gi),i.x=s,i.y=a,i.z=c,this}makePerspective(e,t,i,r,o,s,a=$i,c=!1){let l=this.elements,u=2*o/(t-e),d=2*o/(i-r),f=(t+e)/(t-e),h=(i+r)/(i-r),m,y;if(c)m=o/(s-o),y=s*o/(s-o);else if(a===$i)m=-(s+o)/(s-o),y=-2*s*o/(s-o);else if(a===Oa)m=-s/(s-o),y=-s*o/(s-o);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=f,l[12]=0,l[1]=0,l[5]=d,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=m,l[14]=y,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,o,s,a=$i,c=!1){let l=this.elements,u=2/(t-e),d=2/(i-r),f=-(t+e)/(t-e),h=-(i+r)/(i-r),m,y;if(c)m=1/(s-o),y=s/(s-o);else if(a===$i)m=-2/(s-o),y=-(s+o)/(s-o);else if(a===Oa)m=-1/(s-o),y=-o/(s-o);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=f,l[1]=0,l[5]=d,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=m,l[14]=y,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},Ma=new T,Gi=new Bt,J1=new T(0,0,0),Q1=new T(1,1,1),fo=new T,df=new T,oi=new T,Ab=new Bt,Rb=new ui,fr=(()=>{class n{constructor(t=0,i=0,r=0,o=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=o}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,o=this._order){return this._x=t,this._y=i,this._z=r,this._order=o,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let o=t.elements,s=o[0],a=o[4],c=o[8],l=o[1],u=o[5],d=o[9],f=o[2],h=o[6],m=o[10];switch(i){case"XYZ":this._y=Math.asin(ot(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-d,m),this._z=Math.atan2(-a,s)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-ot(d,-1,1)),Math.abs(d)<.9999999?(this._y=Math.atan2(c,m),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-f,s),this._z=0);break;case"ZXY":this._x=Math.asin(ot(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-f,m),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,s));break;case"ZYX":this._y=Math.asin(-ot(f,-1,1)),Math.abs(f)<.9999999?(this._x=Math.atan2(h,m),this._z=Math.atan2(l,s)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(ot(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-d,u),this._y=Math.atan2(-f,s)):(this._x=0,this._y=Math.atan2(c,m));break;case"XZY":this._z=Math.asin(-ot(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,s)):(this._x=Math.atan2(-d,m),this._y=0);break;default:Fe("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return Ab.makeRotationFromQuaternion(t),this.setFromRotationMatrix(Ab,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return Rb.setFromEuler(this),this.setFromQuaternion(Rb,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),Ba=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},eN=0,Db=new T,ba=new ui,Ar=new Bt,ff=new T,gl=new T,tN=new T,nN=new ui,Nb=new T(1,0,0),Pb=new T(0,1,0),Lb=new T(0,0,1),Ob={type:"added"},iN={type:"removed"},Sa={type:"childadded",child:null},ey={type:"childremoved",child:null},Xn=(()=>{class n extends dr{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:eN++}),this.uuid=Za(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new T,i=new fr,r=new ui,o=new T(1,1,1);function s(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(s),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:o},modelViewMatrix:{value:new Bt},normalMatrix:{value:new et}}),this.matrix=new Bt,this.matrixWorld=new Bt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new Ba,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return ba.setFromAxisAngle(t,i),this.quaternion.multiply(ba),this}rotateOnWorldAxis(t,i){return ba.setFromAxisAngle(t,i),this.quaternion.premultiply(ba),this}rotateX(t){return this.rotateOnAxis(Nb,t)}rotateY(t){return this.rotateOnAxis(Pb,t)}rotateZ(t){return this.rotateOnAxis(Lb,t)}translateOnAxis(t,i){return Db.copy(t).applyQuaternion(this.quaternion),this.position.add(Db.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(Nb,t)}translateY(t){return this.translateOnAxis(Pb,t)}translateZ(t){return this.translateOnAxis(Lb,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(Ar.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?ff.copy(t):ff.set(t,i,r);let o=this.parent;this.updateWorldMatrix(!0,!1),gl.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?Ar.lookAt(gl,ff,this.up):Ar.lookAt(ff,gl,this.up),this.quaternion.setFromRotationMatrix(Ar),o&&(Ar.extractRotation(o.matrixWorld),ba.setFromRotationMatrix(Ar),this.quaternion.premultiply(ba.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(ke("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(Ob),Sa.child=t,this.dispatchEvent(Sa),Sa.child=null):ke("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(iN),ey.child=t,this.dispatchEvent(ey),ey.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),Ar.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),Ar.multiply(t.parent.matrixWorld)),t.applyMatrix4(Ar),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(Ob),Sa.child=t,this.dispatchEvent(Sa),Sa.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,o=this.children.length;r<o;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let o=this.children;for(let s=0,a=o.length;s<a;s++)o[s].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(gl,t,tN),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(gl,nN,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,o=i.length;r<o;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,o=i.length;r<o;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,o=t.z,s=this.matrix.elements;s[12]+=i-s[0]*i-s[4]*r-s[8]*o,s[13]+=r-s[1]*i-s[5]*r-s[9]*o,s[14]+=o-s[2]*i-s[6]*r-s[10]*o}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,o=i.length;r<o;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let o=this.children;for(let s=0,a=o.length;s<a;s++)o[s].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let o={};o.uuid=this.uuid,o.type=this.type,this.name!==""&&(o.name=this.name),this.castShadow===!0&&(o.castShadow=!0),this.receiveShadow===!0&&(o.receiveShadow=!0),this.visible===!1&&(o.visible=!1),this.frustumCulled===!1&&(o.frustumCulled=!1),this.renderOrder!==0&&(o.renderOrder=this.renderOrder),this.static!==!1&&(o.static=this.static),Object.keys(this.userData).length>0&&(o.userData=this.userData),o.layers=this.layers.mask,o.matrix=this.matrix.toArray(),o.up=this.up.toArray(),this.pivot!==null&&(o.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(o.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(o.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(o.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(o.type="InstancedMesh",o.count=this.count,o.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(o.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(o.type="BatchedMesh",o.perObjectFrustumCulled=this.perObjectFrustumCulled,o.sortObjects=this.sortObjects,o.drawRanges=this._drawRanges,o.reservedRanges=this._reservedRanges,o.geometryInfo=this._geometryInfo.map(c=>Ut(Ee({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),o.instanceInfo=this._instanceInfo.map(c=>Ee({},c)),o.availableInstanceIds=this._availableInstanceIds.slice(),o.availableGeometryIds=this._availableGeometryIds.slice(),o.nextIndexStart=this._nextIndexStart,o.nextVertexStart=this._nextVertexStart,o.geometryCount=this._geometryCount,o.maxInstanceCount=this._maxInstanceCount,o.maxVertexCount=this._maxVertexCount,o.maxIndexCount=this._maxIndexCount,o.geometryInitialized=this._geometryInitialized,o.matricesTexture=this._matricesTexture.toJSON(t),o.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(o.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(o.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(o.boundingBox=this.boundingBox.toJSON()));function s(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?o.background=this.background.toJSON():this.background.isTexture&&(o.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(o.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){o.geometry=s(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,d=l.length;u<d;u++){let f=l[u];s(t.shapes,f)}else s(t.shapes,l)}}if(this.isSkinnedMesh&&(o.bindMode=this.bindMode,o.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(s(t.skeletons,this.skeleton),o.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(s(t.materials,this.material[l]));o.material=c}else o.material=s(t.materials,this.material);if(this.children.length>0){o.children=[];for(let c=0;c<this.children.length;c++)o.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){o.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];o.animations.push(s(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),d=a(t.images),f=a(t.shapes),h=a(t.skeletons),m=a(t.animations),y=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),d.length>0&&(r.images=d),f.length>0&&(r.shapes=f),h.length>0&&(r.skeletons=h),m.length>0&&(r.animations=m),y.length>0&&(r.nodes=y)}return r.object=o,r;function a(c){let l=[];for(let u in c){let d=c[u];delete d.metadata,l.push(d)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),t.pivot!==null&&(this.pivot=t.pivot.clone()),this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let o=t.children[r];this.add(o.clone())}return this}}return n.DEFAULT_UP=new T(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),zt=class extends Xn{constructor(){super(),this.isGroup=!0,this.type="Group"}},rN={type:"move"},Ha=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new zt,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new zt,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new T,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new T),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new zt,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new T,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new T),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,o=null,s=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){s=!0;for(let y of e.hand.values()){let g=t.getJointPose(y,i),p=this._getHandJoint(l,y);g!==null&&(p.matrix.fromArray(g.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=g.radius),p.visible=g!==null}let u=l.joints["index-finger-tip"],d=l.joints["thumb-tip"],f=u.position.distanceTo(d.position),h=.02,m=.005;l.inputState.pinching&&f>h+m?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&f<=h-m&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(o=t.getPose(e.gripSpace,i),o!==null&&(c.matrix.fromArray(o.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,o.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(o.linearVelocity)):c.hasLinearVelocity=!1,o.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(o.angularVelocity)):c.hasAngularVelocity=!1));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&o!==null&&(r=o),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(rN)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=o!==null),l!==null&&(l.visible=s!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new zt;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},AS={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},ho={h:0,s:0,l:0},hf={h:0,s:0,l:0};function ty(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Qe=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=ai){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,mt.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=mt.workingColorSpace){return this.r=e,this.g=t,this.b=i,mt.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=mt.workingColorSpace){if(e=Ky(e,1),t=ot(t,0,1),i=ot(i,0,1),t===0)this.r=this.g=this.b=i;else{let o=i<=.5?i*(1+t):i+t-i*t,s=2*i-o;this.r=ty(s,o,e+1/3),this.g=ty(s,o,e),this.b=ty(s,o,e-1/3)}return mt.colorSpaceToWorking(this,r),this}setStyle(e,t=ai){function i(o){o!==void 0&&parseFloat(o)<1&&Fe("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let o,s=r[1],a=r[2];switch(s){case"rgb":case"rgba":if(o=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(o[4]),this.setRGB(Math.min(255,parseInt(o[1],10))/255,Math.min(255,parseInt(o[2],10))/255,Math.min(255,parseInt(o[3],10))/255,t);if(o=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(o[4]),this.setRGB(Math.min(100,parseInt(o[1],10))/100,Math.min(100,parseInt(o[2],10))/100,Math.min(100,parseInt(o[3],10))/100,t);break;case"hsl":case"hsla":if(o=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(o[4]),this.setHSL(parseFloat(o[1])/360,parseFloat(o[2])/100,parseFloat(o[3])/100,t);break;default:Fe("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let o=r[1],s=o.length;if(s===3)return this.setRGB(parseInt(o.charAt(0),16)/15,parseInt(o.charAt(1),16)/15,parseInt(o.charAt(2),16)/15,t);if(s===6)return this.setHex(parseInt(o,16),t);Fe("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=ai){let i=AS[e.toLowerCase()];return i!==void 0?this.setHex(i,t):Fe("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=Lr(e.r),this.g=Lr(e.g),this.b=Lr(e.b),this}copyLinearToSRGB(e){return this.r=La(e.r),this.g=La(e.g),this.b=La(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=ai){return mt.workingToColorSpace(Ln.copy(this),e),Math.round(ot(Ln.r*255,0,255))*65536+Math.round(ot(Ln.g*255,0,255))*256+Math.round(ot(Ln.b*255,0,255))}getHexString(e=ai){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=mt.workingColorSpace){mt.workingToColorSpace(Ln.copy(this),t);let i=Ln.r,r=Ln.g,o=Ln.b,s=Math.max(i,r,o),a=Math.min(i,r,o),c,l,u=(a+s)/2;if(a===s)c=0,l=0;else{let d=s-a;switch(l=u<=.5?d/(s+a):d/(2-s-a),s){case i:c=(r-o)/d+(r<o?6:0);break;case r:c=(o-i)/d+2;break;case o:c=(i-r)/d+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=mt.workingColorSpace){return mt.workingToColorSpace(Ln.copy(this),t),e.r=Ln.r,e.g=Ln.g,e.b=Ln.b,e}getStyle(e=ai){mt.workingToColorSpace(Ln.copy(this),e);let t=Ln.r,i=Ln.g,r=Ln.b;return e!==ai?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(ho),this.setHSL(ho.h+e,ho.s+t,ho.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(ho),e.getHSL(hf);let i=Ml(ho.h,hf.h,t),r=Ml(ho.s,hf.s,t),o=Ml(ho.l,hf.l,t);return this.setHSL(i,r,o),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,o=e.elements;return this.r=o[0]*t+o[3]*i+o[6]*r,this.g=o[1]*t+o[4]*i+o[7]*r,this.b=o[2]*t+o[5]*i+o[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},Ln=new Qe;Qe.NAMES=AS;var Il=class n{constructor(e,t=1,i=1e3){this.isFog=!0,this.name="",this.color=new Qe(e),this.near=t,this.far=i}clone(){return new n(this.color,this.near,this.far)}toJSON(){return{type:"Fog",name:this.name,color:this.color.getHex(),near:this.near,far:this.far}}},Al=class extends Xn{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new fr,this.environmentIntensity=1,this.environmentRotation=new fr,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},Wi=new T,Rr=new T,ny=new T,Dr=new T,wa=new T,Ta=new T,Fb=new T,iy=new T,ry=new T,oy=new T,sy=new jt,ay=new jt,cy=new jt,yo=class n{constructor(e=new T,t=new T,i=new T){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Wi.subVectors(e,t),r.cross(Wi);let o=r.lengthSq();return o>0?r.multiplyScalar(1/Math.sqrt(o)):r.set(0,0,0)}static getBarycoord(e,t,i,r,o){Wi.subVectors(r,t),Rr.subVectors(i,t),ny.subVectors(e,t);let s=Wi.dot(Wi),a=Wi.dot(Rr),c=Wi.dot(ny),l=Rr.dot(Rr),u=Rr.dot(ny),d=s*l-a*a;if(d===0)return o.set(0,0,0),null;let f=1/d,h=(l*c-a*u)*f,m=(s*u-a*c)*f;return o.set(1-h-m,m,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,Dr)===null?!1:Dr.x>=0&&Dr.y>=0&&Dr.x+Dr.y<=1}static getInterpolation(e,t,i,r,o,s,a,c){return this.getBarycoord(e,t,i,r,Dr)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(o,Dr.x),c.addScaledVector(s,Dr.y),c.addScaledVector(a,Dr.z),c)}static getInterpolatedAttribute(e,t,i,r,o,s){return sy.setScalar(0),ay.setScalar(0),cy.setScalar(0),sy.fromBufferAttribute(e,t),ay.fromBufferAttribute(e,i),cy.fromBufferAttribute(e,r),s.setScalar(0),s.addScaledVector(sy,o.x),s.addScaledVector(ay,o.y),s.addScaledVector(cy,o.z),s}static isFrontFacing(e,t,i,r){return Wi.subVectors(i,t),Rr.subVectors(e,t),Wi.cross(Rr).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Wi.subVectors(this.c,this.b),Rr.subVectors(this.a,this.b),Wi.cross(Rr).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,o){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,o)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,o=this.c,s,a;wa.subVectors(r,i),Ta.subVectors(o,i),iy.subVectors(e,i);let c=wa.dot(iy),l=Ta.dot(iy);if(c<=0&&l<=0)return t.copy(i);ry.subVectors(e,r);let u=wa.dot(ry),d=Ta.dot(ry);if(u>=0&&d<=u)return t.copy(r);let f=c*d-u*l;if(f<=0&&c>=0&&u<=0)return s=c/(c-u),t.copy(i).addScaledVector(wa,s);oy.subVectors(e,o);let h=wa.dot(oy),m=Ta.dot(oy);if(m>=0&&h<=m)return t.copy(o);let y=h*l-c*m;if(y<=0&&l>=0&&m<=0)return a=l/(l-m),t.copy(i).addScaledVector(Ta,a);let g=u*m-h*d;if(g<=0&&d-u>=0&&h-m>=0)return Fb.subVectors(o,r),a=(d-u)/(d-u+(h-m)),t.copy(r).addScaledVector(Fb,a);let p=1/(g+y+f);return s=y*p,a=f*p,t.copy(i).addScaledVector(wa,s).addScaledVector(Ta,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},xo=class{constructor(e=new T(1/0,1/0,1/0),t=new T(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(ji.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(ji.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=ji.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let o=i.getAttribute("position");if(t===!0&&o!==void 0&&e.isInstancedMesh!==!0)for(let s=0,a=o.count;s<a;s++)e.isMesh===!0?e.getVertexPosition(s,ji):ji.fromBufferAttribute(o,s),ji.applyMatrix4(e.matrixWorld),this.expandByPoint(ji);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),pf.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),pf.copy(i.boundingBox)),pf.applyMatrix4(e.matrixWorld),this.union(pf)}let r=e.children;for(let o=0,s=r.length;o<s;o++)this.expandByObject(r[o],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,ji),ji.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(vl),mf.subVectors(this.max,vl),Ca.subVectors(e.a,vl),Ia.subVectors(e.b,vl),Aa.subVectors(e.c,vl),po.subVectors(Ia,Ca),mo.subVectors(Aa,Ia),cs.subVectors(Ca,Aa);let t=[0,-po.z,po.y,0,-mo.z,mo.y,0,-cs.z,cs.y,po.z,0,-po.x,mo.z,0,-mo.x,cs.z,0,-cs.x,-po.y,po.x,0,-mo.y,mo.x,0,-cs.y,cs.x,0];return!ly(t,Ca,Ia,Aa,mf)||(t=[1,0,0,0,1,0,0,0,1],!ly(t,Ca,Ia,Aa,mf))?!1:(gf.crossVectors(po,mo),t=[gf.x,gf.y,gf.z],ly(t,Ca,Ia,Aa,mf))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,ji).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(ji).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(Nr[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),Nr[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),Nr[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),Nr[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),Nr[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),Nr[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),Nr[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),Nr[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(Nr),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},Nr=[new T,new T,new T,new T,new T,new T,new T,new T],ji=new T,pf=new xo,Ca=new T,Ia=new T,Aa=new T,po=new T,mo=new T,cs=new T,vl=new T,mf=new T,gf=new T,ls=new T;function ly(n,e,t,i,r){for(let o=0,s=n.length-3;o<=s;o+=3){ls.fromArray(n,o);let a=r.x*Math.abs(ls.x)+r.y*Math.abs(ls.y)+r.z*Math.abs(ls.z),c=e.dot(ls),l=t.dot(ls),u=i.dot(ls);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var nn=new T,vf=new Ue,oN=0,vn=class{constructor(e,t,i=!1){if(Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:oN++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=wy,this.updateRanges=[],this.gpuType=Yi,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,o=this.itemSize;r<o;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)vf.fromBufferAttribute(this,t),vf.applyMatrix3(e),this.setXY(t,vf.x,vf.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)nn.fromBufferAttribute(this,t),nn.applyMatrix3(e),this.setXYZ(t,nn.x,nn.y,nn.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)nn.fromBufferAttribute(this,t),nn.applyMatrix4(e),this.setXYZ(t,nn.x,nn.y,nn.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)nn.fromBufferAttribute(this,t),nn.applyNormalMatrix(e),this.setXYZ(t,nn.x,nn.y,nn.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)nn.fromBufferAttribute(this,t),nn.transformDirection(e),this.setXYZ(t,nn.x,nn.y,nn.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=Pa(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=Vn(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=Pa(t,this.array)),t}setX(e,t){return this.normalized&&(t=Vn(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=Pa(t,this.array)),t}setY(e,t){return this.normalized&&(t=Vn(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=Pa(t,this.array)),t}setZ(e,t){return this.normalized&&(t=Vn(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=Pa(t,this.array)),t}setW(e,t){return this.normalized&&(t=Vn(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=Vn(t,this.array),i=Vn(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=Vn(t,this.array),i=Vn(i,this.array),r=Vn(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,o){return e*=this.itemSize,this.normalized&&(t=Vn(t,this.array),i=Vn(i,this.array),r=Vn(r,this.array),o=Vn(o,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=o,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==wy&&(e.usage=this.usage),e}};var Rl=class extends vn{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Dl=class extends vn{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var ci=class extends vn{constructor(e,t,i){super(new Float32Array(e),t,i)}},sN=new xo,yl=new T,uy=new T,gs=class{constructor(e=new T,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):sN.setFromPoints(e).getCenter(i);let r=0;for(let o=0,s=e.length;o<s;o++)r=Math.max(r,i.distanceToSquared(e[o]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;yl.subVectors(e,this.center);let t=yl.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(yl,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(uy.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(yl.copy(e.center).add(uy)),this.expandByPoint(yl.copy(e.center).sub(uy))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},aN=0,Si=new Bt,dy=new Xn,Ra=new T,si=new xo,_l=new xo,gn=new T,_n=class n extends dr{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:aN++}),this.uuid=Za(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(D1(e)?Dl:Rl)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let o=new et().getNormalMatrix(e);i.applyNormalMatrix(o),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return Si.makeRotationFromQuaternion(e),this.applyMatrix4(Si),this}rotateX(e){return Si.makeRotationX(e),this.applyMatrix4(Si),this}rotateY(e){return Si.makeRotationY(e),this.applyMatrix4(Si),this}rotateZ(e){return Si.makeRotationZ(e),this.applyMatrix4(Si),this}translate(e,t,i){return Si.makeTranslation(e,t,i),this.applyMatrix4(Si),this}scale(e,t,i){return Si.makeScale(e,t,i),this.applyMatrix4(Si),this}lookAt(e){return dy.lookAt(e),dy.updateMatrix(),this.applyMatrix4(dy.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Ra).negate(),this.translate(Ra.x,Ra.y,Ra.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,o=e.length;r<o;r++){let s=e[r];i.push(s.x,s.y,s.z||0)}this.setAttribute("position",new ci(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let o=e[r];t.setXYZ(r,o.x,o.y,o.z||0)}e.length>t.count&&Fe("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new xo);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){ke("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new T(-1/0,-1/0,-1/0),new T(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let o=t[i];si.setFromBufferAttribute(o),this.morphTargetsRelative?(gn.addVectors(this.boundingBox.min,si.min),this.boundingBox.expandByPoint(gn),gn.addVectors(this.boundingBox.max,si.max),this.boundingBox.expandByPoint(gn)):(this.boundingBox.expandByPoint(si.min),this.boundingBox.expandByPoint(si.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&ke('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new gs);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){ke("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new T,1/0);return}if(e){let i=this.boundingSphere.center;if(si.setFromBufferAttribute(e),t)for(let o=0,s=t.length;o<s;o++){let a=t[o];_l.setFromBufferAttribute(a),this.morphTargetsRelative?(gn.addVectors(si.min,_l.min),si.expandByPoint(gn),gn.addVectors(si.max,_l.max),si.expandByPoint(gn)):(si.expandByPoint(_l.min),si.expandByPoint(_l.max))}si.getCenter(i);let r=0;for(let o=0,s=e.count;o<s;o++)gn.fromBufferAttribute(e,o),r=Math.max(r,i.distanceToSquared(gn));if(t)for(let o=0,s=t.length;o<s;o++){let a=t[o],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)gn.fromBufferAttribute(a,l),c&&(Ra.fromBufferAttribute(e,l),gn.add(Ra)),r=Math.max(r,i.distanceToSquared(gn))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&ke('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){ke("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,o=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new vn(new Float32Array(4*i.count),4));let s=this.getAttribute("tangent"),a=[],c=[];for(let _=0;_<i.count;_++)a[_]=new T,c[_]=new T;let l=new T,u=new T,d=new T,f=new Ue,h=new Ue,m=new Ue,y=new T,g=new T;function p(_,M,q){l.fromBufferAttribute(i,_),u.fromBufferAttribute(i,M),d.fromBufferAttribute(i,q),f.fromBufferAttribute(o,_),h.fromBufferAttribute(o,M),m.fromBufferAttribute(o,q),u.sub(l),d.sub(l),h.sub(f),m.sub(f);let C=1/(h.x*m.y-m.x*h.y);isFinite(C)&&(y.copy(u).multiplyScalar(m.y).addScaledVector(d,-h.y).multiplyScalar(C),g.copy(d).multiplyScalar(h.x).addScaledVector(u,-m.x).multiplyScalar(C),a[_].add(y),a[M].add(y),a[q].add(y),c[_].add(g),c[M].add(g),c[q].add(g))}let E=this.groups;E.length===0&&(E=[{start:0,count:e.count}]);for(let _=0,M=E.length;_<M;++_){let q=E[_],C=q.start,U=q.count;for(let B=C,W=C+U;B<W;B+=3)p(e.getX(B+0),e.getX(B+1),e.getX(B+2))}let w=new T,S=new T,I=new T,R=new T;function D(_){I.fromBufferAttribute(r,_),R.copy(I);let M=a[_];w.copy(M),w.sub(I.multiplyScalar(I.dot(M))).normalize(),S.crossVectors(R,M);let C=S.dot(c[_])<0?-1:1;s.setXYZW(_,w.x,w.y,w.z,C)}for(let _=0,M=E.length;_<M;++_){let q=E[_],C=q.start,U=q.count;for(let B=C,W=C+U;B<W;B+=3)D(e.getX(B+0)),D(e.getX(B+1)),D(e.getX(B+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new vn(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let f=0,h=i.count;f<h;f++)i.setXYZ(f,0,0,0);let r=new T,o=new T,s=new T,a=new T,c=new T,l=new T,u=new T,d=new T;if(e)for(let f=0,h=e.count;f<h;f+=3){let m=e.getX(f+0),y=e.getX(f+1),g=e.getX(f+2);r.fromBufferAttribute(t,m),o.fromBufferAttribute(t,y),s.fromBufferAttribute(t,g),u.subVectors(s,o),d.subVectors(r,o),u.cross(d),a.fromBufferAttribute(i,m),c.fromBufferAttribute(i,y),l.fromBufferAttribute(i,g),a.add(u),c.add(u),l.add(u),i.setXYZ(m,a.x,a.y,a.z),i.setXYZ(y,c.x,c.y,c.z),i.setXYZ(g,l.x,l.y,l.z)}else for(let f=0,h=t.count;f<h;f+=3)r.fromBufferAttribute(t,f+0),o.fromBufferAttribute(t,f+1),s.fromBufferAttribute(t,f+2),u.subVectors(s,o),d.subVectors(r,o),u.cross(d),i.setXYZ(f+0,u.x,u.y,u.z),i.setXYZ(f+1,u.x,u.y,u.z),i.setXYZ(f+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)gn.fromBufferAttribute(e,t),gn.normalize(),e.setXYZ(t,gn.x,gn.y,gn.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,d=a.normalized,f=new l.constructor(c.length*u),h=0,m=0;for(let y=0,g=c.length;y<g;y++){a.isInterleavedBufferAttribute?h=c[y]*a.data.stride+a.offset:h=c[y]*u;for(let p=0;p<u;p++)f[m++]=l[h++]}return new vn(f,u,d)}if(this.index===null)return Fe("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let o=this.morphAttributes;for(let a in o){let c=[],l=o[a];for(let u=0,d=l.length;u<d;u++){let f=l[u],h=e(f,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let s=this.groups;for(let a=0,c=s.length;a<c;a++){let l=s[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},o=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let d=0,f=l.length;d<f;d++){let h=l[d];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,o=!0)}o&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let s=this.groups;s.length>0&&(e.data.groups=JSON.parse(JSON.stringify(s)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let o=e.morphAttributes;for(let l in o){let u=[],d=o[l];for(let f=0,h=d.length;f<h;f++)u.push(d[f].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let s=e.groups;for(let l=0,u=s.length;l<u;l++){let d=s[l];this.addGroup(d.start,d.count,d.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var cN=0,Fr=class extends dr{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:cN++}),this.uuid=Za(),this.name="",this.type="Material",this.blending=hs,this.side=Or,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=Lf,this.blendDst=Of,this.blendEquation=_o,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Qe(0,0,0),this.blendAlpha=0,this.depthFunc=ps,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Sy,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=fs,this.stencilZFail=fs,this.stencilZPass=fs,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){Fe(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){Fe(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==hs&&(i.blending=this.blending),this.side!==Or&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==Lf&&(i.blendSrc=this.blendSrc),this.blendDst!==Of&&(i.blendDst=this.blendDst),this.blendEquation!==_o&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==ps&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Sy&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==fs&&(i.stencilFail=this.stencilFail),this.stencilZFail!==fs&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==fs&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(o){let s=[];for(let a in o){let c=o[a];delete c.metadata,s.push(c)}return s}if(t){let o=r(e.textures),s=r(e.images);o.length>0&&(i.textures=o),s.length>0&&(i.images=s)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let o=0;o!==r;++o)i[o]=t[o].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var Pr=new T,fy=new T,yf=new T,go=new T,hy=new T,_f=new T,py=new T,Eo=class{constructor(e=new T,t=new T(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,Pr)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=Pr.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(Pr.copy(this.origin).addScaledVector(this.direction,t),Pr.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){fy.copy(e).add(t).multiplyScalar(.5),yf.copy(t).sub(e).normalize(),go.copy(this.origin).sub(fy);let o=e.distanceTo(t)*.5,s=-this.direction.dot(yf),a=go.dot(this.direction),c=-go.dot(yf),l=go.lengthSq(),u=Math.abs(1-s*s),d,f,h,m;if(u>0)if(d=s*c-a,f=s*a-c,m=o*u,d>=0)if(f>=-m)if(f<=m){let y=1/u;d*=y,f*=y,h=d*(d+s*f+2*a)+f*(s*d+f+2*c)+l}else f=o,d=Math.max(0,-(s*f+a)),h=-d*d+f*(f+2*c)+l;else f=-o,d=Math.max(0,-(s*f+a)),h=-d*d+f*(f+2*c)+l;else f<=-m?(d=Math.max(0,-(-s*o+a)),f=d>0?-o:Math.min(Math.max(-o,-c),o),h=-d*d+f*(f+2*c)+l):f<=m?(d=0,f=Math.min(Math.max(-o,-c),o),h=f*(f+2*c)+l):(d=Math.max(0,-(s*o+a)),f=d>0?o:Math.min(Math.max(-o,-c),o),h=-d*d+f*(f+2*c)+l);else f=s>0?-o:o,d=Math.max(0,-(s*f+a)),h=-d*d+f*(f+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,d),r&&r.copy(fy).addScaledVector(yf,f),h}intersectSphere(e,t){Pr.subVectors(e.center,this.origin);let i=Pr.dot(this.direction),r=Pr.dot(Pr)-i*i,o=e.radius*e.radius;if(r>o)return null;let s=Math.sqrt(o-r),a=i-s,c=i+s;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,o,s,a,c,l=1/this.direction.x,u=1/this.direction.y,d=1/this.direction.z,f=this.origin;return l>=0?(i=(e.min.x-f.x)*l,r=(e.max.x-f.x)*l):(i=(e.max.x-f.x)*l,r=(e.min.x-f.x)*l),u>=0?(o=(e.min.y-f.y)*u,s=(e.max.y-f.y)*u):(o=(e.max.y-f.y)*u,s=(e.min.y-f.y)*u),i>s||o>r||((o>i||isNaN(i))&&(i=o),(s<r||isNaN(r))&&(r=s),d>=0?(a=(e.min.z-f.z)*d,c=(e.max.z-f.z)*d):(a=(e.max.z-f.z)*d,c=(e.min.z-f.z)*d),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,Pr)!==null}intersectTriangle(e,t,i,r,o){hy.subVectors(t,e),_f.subVectors(i,e),py.crossVectors(hy,_f);let s=this.direction.dot(py),a;if(s>0){if(r)return null;a=1}else if(s<0)a=-1,s=-s;else return null;go.subVectors(this.origin,e);let c=a*this.direction.dot(_f.crossVectors(go,_f));if(c<0)return null;let l=a*this.direction.dot(hy.cross(go));if(l<0||c+l>s)return null;let u=-a*go.dot(py);return u<0?null:this.at(u/s,o)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},vs=class extends Fr{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Qe(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new fr,this.combine=Ly,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},kb=new Bt,us=new Eo,xf=new gs,Ub=new T,Ef=new T,Mf=new T,bf=new T,my=new T,Sf=new T,Bb=new T,wf=new T,Me=class extends Xn{constructor(e=new _n,t=new vs){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let o=0,s=r.length;o<s;o++){let a=r[o].name||String(o);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=o}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,o=i.morphAttributes.position,s=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(o&&a){Sf.set(0,0,0);for(let c=0,l=o.length;c<l;c++){let u=a[c],d=o[c];u!==0&&(my.fromBufferAttribute(d,e),s?Sf.addScaledVector(my,u):Sf.addScaledVector(my.sub(t),u))}t.add(Sf)}return t}raycast(e,t){let i=this.geometry,r=this.material,o=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),xf.copy(i.boundingSphere),xf.applyMatrix4(o),us.copy(e.ray).recast(e.near),!(xf.containsPoint(us.origin)===!1&&(us.intersectSphere(xf,Ub)===null||us.origin.distanceToSquared(Ub)>(e.far-e.near)**2))&&(kb.copy(o).invert(),us.copy(e.ray).applyMatrix4(kb),!(i.boundingBox!==null&&us.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,us)))}_computeIntersections(e,t,i){let r,o=this.geometry,s=this.material,a=o.index,c=o.attributes.position,l=o.attributes.uv,u=o.attributes.uv1,d=o.attributes.normal,f=o.groups,h=o.drawRange;if(a!==null)if(Array.isArray(s))for(let m=0,y=f.length;m<y;m++){let g=f[m],p=s[g.materialIndex],E=Math.max(g.start,h.start),w=Math.min(a.count,Math.min(g.start+g.count,h.start+h.count));for(let S=E,I=w;S<I;S+=3){let R=a.getX(S),D=a.getX(S+1),_=a.getX(S+2);r=Tf(this,p,e,i,l,u,d,R,D,_),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let m=Math.max(0,h.start),y=Math.min(a.count,h.start+h.count);for(let g=m,p=y;g<p;g+=3){let E=a.getX(g),w=a.getX(g+1),S=a.getX(g+2);r=Tf(this,s,e,i,l,u,d,E,w,S),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(s))for(let m=0,y=f.length;m<y;m++){let g=f[m],p=s[g.materialIndex],E=Math.max(g.start,h.start),w=Math.min(c.count,Math.min(g.start+g.count,h.start+h.count));for(let S=E,I=w;S<I;S+=3){let R=S,D=S+1,_=S+2;r=Tf(this,p,e,i,l,u,d,R,D,_),r&&(r.faceIndex=Math.floor(S/3),r.face.materialIndex=g.materialIndex,t.push(r))}}else{let m=Math.max(0,h.start),y=Math.min(c.count,h.start+h.count);for(let g=m,p=y;g<p;g+=3){let E=g,w=g+1,S=g+2;r=Tf(this,s,e,i,l,u,d,E,w,S),r&&(r.faceIndex=Math.floor(g/3),t.push(r))}}}};function lN(n,e,t,i,r,o,s,a){let c;if(e.side===zn?c=i.intersectTriangle(s,o,r,!0,a):c=i.intersectTriangle(r,o,s,e.side===Or,a),c===null)return null;wf.copy(a),wf.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(wf);return l<t.near||l>t.far?null:{distance:l,point:wf.clone(),object:n}}function Tf(n,e,t,i,r,o,s,a,c,l){n.getVertexPosition(a,Ef),n.getVertexPosition(c,Mf),n.getVertexPosition(l,bf);let u=lN(n,e,t,i,Ef,Mf,bf,Bb);if(u){let d=new T;yo.getBarycoord(Bb,Ef,Mf,bf,d),r&&(u.uv=yo.getInterpolatedAttribute(r,a,c,l,d,new Ue)),o&&(u.uv1=yo.getInterpolatedAttribute(o,a,c,l,d,new Ue)),s&&(u.normal=yo.getInterpolatedAttribute(s,a,c,l,d,new T),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let f={a,b:c,c:l,normal:new T,materialIndex:0};yo.getNormal(Ef,Mf,bf,f.normal),u.face=f,u.barycoord=d}return u}var Xf=class extends mr{constructor(e=null,t=1,i=1,r,o,s,a,c,l=yn,u=yn,d,f){super(null,s,a,c,l,u,r,o,d,f),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var gy=new T,uN=new T,dN=new et,wi=class{constructor(e=new T(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=gy.subVectors(i,t).cross(uN.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t){let i=e.delta(gy),r=this.normal.dot(i);if(r===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/r;return o<0||o>1?null:t.copy(e.start).addScaledVector(i,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||dN.getNormalMatrix(e),r=this.coplanarPoint(gy).applyMatrix4(e),o=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(o),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},ds=new gs,fN=new Ue(.5,.5),Cf=new T,Va=class{constructor(e=new wi,t=new wi,i=new wi,r=new wi,o=new wi,s=new wi){this.planes=[e,t,i,r,o,s]}set(e,t,i,r,o,s){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(o),a[5].copy(s),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=$i,i=!1){let r=this.planes,o=e.elements,s=o[0],a=o[1],c=o[2],l=o[3],u=o[4],d=o[5],f=o[6],h=o[7],m=o[8],y=o[9],g=o[10],p=o[11],E=o[12],w=o[13],S=o[14],I=o[15];if(r[0].setComponents(l-s,h-u,p-m,I-E).normalize(),r[1].setComponents(l+s,h+u,p+m,I+E).normalize(),r[2].setComponents(l+a,h+d,p+y,I+w).normalize(),r[3].setComponents(l-a,h-d,p-y,I-w).normalize(),i)r[4].setComponents(c,f,g,S).normalize(),r[5].setComponents(l-c,h-f,p-g,I-S).normalize();else if(r[4].setComponents(l-c,h-f,p-g,I-S).normalize(),t===$i)r[5].setComponents(l+c,h+f,p+g,I+S).normalize();else if(t===Oa)r[5].setComponents(c,f,g,S).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),ds.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),ds.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(ds)}intersectsSprite(e){ds.center.set(0,0,0);let t=fN.distanceTo(e.center);return ds.radius=.7071067811865476+t,ds.applyMatrix4(e.matrixWorld),this.intersectsSphere(ds)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let o=0;o<6;o++)if(t[o].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Cf.x=r.normal.x>0?e.max.x:e.min.x,Cf.y=r.normal.y>0?e.max.y:e.min.y,Cf.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Cf)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var ys=class extends Fr{constructor(e){super(),this.isLineBasicMaterial=!0,this.type="LineBasicMaterial",this.color=new Qe(16777215),this.map=null,this.linewidth=1,this.linecap="round",this.linejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.linewidth=e.linewidth,this.linecap=e.linecap,this.linejoin=e.linejoin,this.fog=e.fog,this}},Yf=new T,Zf=new T,Hb=new Bt,xl=new Eo,If=new gs,vy=new T,Vb=new T,Nl=class extends Xn{constructor(e=new _n,t=new ys){super(),this.isLine=!0,this.type="Line",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}computeLineDistances(){let e=this.geometry;if(e.index===null){let t=e.attributes.position,i=[0];for(let r=1,o=t.count;r<o;r++)Yf.fromBufferAttribute(t,r-1),Zf.fromBufferAttribute(t,r),i[r]=i[r-1],i[r]+=Yf.distanceTo(Zf);e.setAttribute("lineDistance",new ci(i,1))}else Fe("Line.computeLineDistances(): Computation only possible with non-indexed BufferGeometry.");return this}raycast(e,t){let i=this.geometry,r=this.matrixWorld,o=e.params.Line.threshold,s=i.drawRange;if(i.boundingSphere===null&&i.computeBoundingSphere(),If.copy(i.boundingSphere),If.applyMatrix4(r),If.radius+=o,e.ray.intersectsSphere(If)===!1)return;Hb.copy(r).invert(),xl.copy(e.ray).applyMatrix4(Hb);let a=o/((this.scale.x+this.scale.y+this.scale.z)/3),c=a*a,l=this.isLineSegments?2:1,u=i.index,f=i.attributes.position;if(u!==null){let h=Math.max(0,s.start),m=Math.min(u.count,s.start+s.count);for(let y=h,g=m-1;y<g;y+=l){let p=u.getX(y),E=u.getX(y+1),w=Af(this,e,xl,c,p,E,y);w&&t.push(w)}if(this.isLineLoop){let y=u.getX(m-1),g=u.getX(h),p=Af(this,e,xl,c,y,g,m-1);p&&t.push(p)}}else{let h=Math.max(0,s.start),m=Math.min(f.count,s.start+s.count);for(let y=h,g=m-1;y<g;y+=l){let p=Af(this,e,xl,c,y,y+1,y);p&&t.push(p)}if(this.isLineLoop){let y=Af(this,e,xl,c,m-1,h,m-1);y&&t.push(y)}}}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let o=0,s=r.length;o<s;o++){let a=r[o].name||String(o);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=o}}}}};function Af(n,e,t,i,r,o,s){let a=n.geometry.attributes.position;if(Yf.fromBufferAttribute(a,r),Zf.fromBufferAttribute(a,o),t.distanceSqToSegment(Yf,Zf,vy,Vb)>i)return;vy.applyMatrix4(n.matrixWorld);let l=e.ray.origin.distanceTo(vy);if(!(l<e.near||l>e.far))return{distance:l,point:Vb.clone().applyMatrix4(n.matrixWorld),index:s,face:null,faceIndex:null,barycoord:null,object:n}}var Pl=class extends mr{constructor(e=[],t=Ao,i,r,o,s,a,c,l,u){super(e,t,i,r,o,s,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}},Mo=class extends mr{constructor(e,t,i,r,o,s,a,c,l){super(e,t,i,r,o,s,a,c,l),this.isCanvasTexture=!0,this.needsUpdate=!0}},bo=class extends mr{constructor(e,t,i=Xi,r,o,s,a=yn,c=yn,l,u=ur,d=1){if(u!==ur&&u!==Do)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let f={width:e,height:t,depth:d};super(f,r,o,s,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new Ua(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},Kf=class extends bo{constructor(e,t=Xi,i=Ao,r,o,s=yn,a=yn,c,l=ur){let u={width:e,height:e,depth:1},d=[u,u,u,u,u,u];super(e,e,t,i,r,o,s,a,c,l),this.image=d,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Ll=class extends mr{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},ct=class n extends _n{constructor(e=1,t=1,i=1,r=1,o=1,s=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:o,depthSegments:s};let a=this;r=Math.floor(r),o=Math.floor(o),s=Math.floor(s);let c=[],l=[],u=[],d=[],f=0,h=0;m("z","y","x",-1,-1,i,t,e,s,o,0),m("z","y","x",1,-1,i,t,-e,s,o,1),m("x","z","y",1,1,e,i,t,r,s,2),m("x","z","y",1,-1,e,i,-t,r,s,3),m("x","y","z",1,-1,e,t,i,r,o,4),m("x","y","z",-1,-1,e,t,-i,r,o,5),this.setIndex(c),this.setAttribute("position",new ci(l,3)),this.setAttribute("normal",new ci(u,3)),this.setAttribute("uv",new ci(d,2));function m(y,g,p,E,w,S,I,R,D,_,M){let q=S/D,C=I/_,U=S/2,B=I/2,W=R/2,H=D+1,V=_+1,O=0,ee=0,Z=new T;for(let fe=0;fe<V;fe++){let ye=fe*C-B;for(let pe=0;pe<H;pe++){let We=pe*q-U;Z[y]=We*E,Z[g]=ye*w,Z[p]=W,l.push(Z.x,Z.y,Z.z),Z[y]=0,Z[g]=0,Z[p]=R>0?1:-1,u.push(Z.x,Z.y,Z.z),d.push(pe/D),d.push(1-fe/_),O+=1}}for(let fe=0;fe<_;fe++)for(let ye=0;ye<D;ye++){let pe=f+ye+H*fe,We=f+ye+H*(fe+1),Dt=f+(ye+1)+H*(fe+1),wt=f+(ye+1)+H*fe;c.push(pe,We,wt),c.push(We,Dt,wt),ee+=6}a.addGroup(h,ee,M),h+=ee,f+=O}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var Wt=class n extends _n{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let o=e/2,s=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,d=e/a,f=t/c,h=[],m=[],y=[],g=[];for(let p=0;p<u;p++){let E=p*f-s;for(let w=0;w<l;w++){let S=w*d-o;m.push(S,-E,0),y.push(0,0,1),g.push(w/a),g.push(1-p/c)}}for(let p=0;p<c;p++)for(let E=0;E<a;E++){let w=E+l*p,S=E+l*(p+1),I=E+1+l*(p+1),R=E+1+l*p;h.push(w,S,R),h.push(S,I,R)}this.setIndex(h),this.setAttribute("position",new ci(m,3)),this.setAttribute("normal",new ci(y,3)),this.setAttribute("uv",new ci(g,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function Ms(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];r&&(r.isColor||r.isMatrix3||r.isMatrix4||r.isVector2||r.isVector3||r.isVector4||r.isTexture||r.isQuaternion)?r.isRenderTargetTexture?(Fe("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone():Array.isArray(r)?e[t][i]=r.slice():e[t][i]=r}}return e}function Fn(n){let e={};for(let t=0;t<n.length;t++){let i=Ms(n[t]);for(let r in i)e[r]=i[r]}return e}function hN(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function Qy(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:mt.workingColorSpace}var RS={clone:Ms,merge:Fn},pN=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,mN=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,fi=class extends Fr{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=pN,this.fragmentShader=mN,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Ms(e.uniforms),this.uniformsGroups=hN(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let s=this.uniforms[r].value;s&&s.isTexture?t.uniforms[r]={type:"t",value:s.toJSON(e).uuid}:s&&s.isColor?t.uniforms[r]={type:"c",value:s.getHex()}:s&&s.isVector2?t.uniforms[r]={type:"v2",value:s.toArray()}:s&&s.isVector3?t.uniforms[r]={type:"v3",value:s.toArray()}:s&&s.isVector4?t.uniforms[r]={type:"v4",value:s.toArray()}:s&&s.isMatrix3?t.uniforms[r]={type:"m3",value:s.toArray()}:s&&s.isMatrix4?t.uniforms[r]={type:"m4",value:s.toArray()}:t.uniforms[r]={value:s}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Jf=class extends fi{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}},St=class extends Fr{constructor(e){super(),this.isMeshStandardMaterial=!0,this.type="MeshStandardMaterial",this.defines={STANDARD:""},this.color=new Qe(16777215),this.roughness=1,this.metalness=0,this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.emissive=new Qe(0),this.emissiveIntensity=1,this.emissiveMap=null,this.bumpMap=null,this.bumpScale=1,this.normalMap=null,this.normalMapType=Xy,this.normalScale=new Ue(1,1),this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.roughnessMap=null,this.metalnessMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new fr,this.envMapIntensity=1,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.flatShading=!1,this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.defines={STANDARD:""},this.color.copy(e.color),this.roughness=e.roughness,this.metalness=e.metalness,this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.emissive.copy(e.emissive),this.emissiveMap=e.emissiveMap,this.emissiveIntensity=e.emissiveIntensity,this.bumpMap=e.bumpMap,this.bumpScale=e.bumpScale,this.normalMap=e.normalMap,this.normalMapType=e.normalMapType,this.normalScale.copy(e.normalScale),this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.roughnessMap=e.roughnessMap,this.metalnessMap=e.metalnessMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.envMapIntensity=e.envMapIntensity,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.flatShading=e.flatShading,this.fog=e.fog,this}},za=class extends St{constructor(e){super(),this.isMeshPhysicalMaterial=!0,this.defines={STANDARD:"",PHYSICAL:""},this.type="MeshPhysicalMaterial",this.anisotropyRotation=0,this.anisotropyMap=null,this.clearcoatMap=null,this.clearcoatRoughness=0,this.clearcoatRoughnessMap=null,this.clearcoatNormalScale=new Ue(1,1),this.clearcoatNormalMap=null,this.ior=1.5,Object.defineProperty(this,"reflectivity",{get:function(){return ot(2.5*(this.ior-1)/(this.ior+1),0,1)},set:function(t){this.ior=(1+.4*t)/(1-.4*t)}}),this.iridescenceMap=null,this.iridescenceIOR=1.3,this.iridescenceThicknessRange=[100,400],this.iridescenceThicknessMap=null,this.sheenColor=new Qe(0),this.sheenColorMap=null,this.sheenRoughness=1,this.sheenRoughnessMap=null,this.transmissionMap=null,this.thickness=0,this.thicknessMap=null,this.attenuationDistance=1/0,this.attenuationColor=new Qe(1,1,1),this.specularIntensity=1,this.specularIntensityMap=null,this.specularColor=new Qe(1,1,1),this.specularColorMap=null,this._anisotropy=0,this._clearcoat=0,this._dispersion=0,this._iridescence=0,this._sheen=0,this._transmission=0,this.setValues(e)}get anisotropy(){return this._anisotropy}set anisotropy(e){this._anisotropy>0!=e>0&&this.version++,this._anisotropy=e}get clearcoat(){return this._clearcoat}set clearcoat(e){this._clearcoat>0!=e>0&&this.version++,this._clearcoat=e}get iridescence(){return this._iridescence}set iridescence(e){this._iridescence>0!=e>0&&this.version++,this._iridescence=e}get dispersion(){return this._dispersion}set dispersion(e){this._dispersion>0!=e>0&&this.version++,this._dispersion=e}get sheen(){return this._sheen}set sheen(e){this._sheen>0!=e>0&&this.version++,this._sheen=e}get transmission(){return this._transmission}set transmission(e){this._transmission>0!=e>0&&this.version++,this._transmission=e}copy(e){return super.copy(e),this.defines={STANDARD:"",PHYSICAL:""},this.anisotropy=e.anisotropy,this.anisotropyRotation=e.anisotropyRotation,this.anisotropyMap=e.anisotropyMap,this.clearcoat=e.clearcoat,this.clearcoatMap=e.clearcoatMap,this.clearcoatRoughness=e.clearcoatRoughness,this.clearcoatRoughnessMap=e.clearcoatRoughnessMap,this.clearcoatNormalMap=e.clearcoatNormalMap,this.clearcoatNormalScale.copy(e.clearcoatNormalScale),this.dispersion=e.dispersion,this.ior=e.ior,this.iridescence=e.iridescence,this.iridescenceMap=e.iridescenceMap,this.iridescenceIOR=e.iridescenceIOR,this.iridescenceThicknessRange=[...e.iridescenceThicknessRange],this.iridescenceThicknessMap=e.iridescenceThicknessMap,this.sheen=e.sheen,this.sheenColor.copy(e.sheenColor),this.sheenColorMap=e.sheenColorMap,this.sheenRoughness=e.sheenRoughness,this.sheenRoughnessMap=e.sheenRoughnessMap,this.transmission=e.transmission,this.transmissionMap=e.transmissionMap,this.thickness=e.thickness,this.thicknessMap=e.thicknessMap,this.attenuationDistance=e.attenuationDistance,this.attenuationColor.copy(e.attenuationColor),this.specularIntensity=e.specularIntensity,this.specularIntensityMap=e.specularIntensityMap,this.specularColor.copy(e.specularColor),this.specularColorMap=e.specularColorMap,this}};var Qf=class extends Fr{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=vS,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},eh=class extends Fr{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function Rf(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var So=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],o=t[i-1];n:{e:{let s;t:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<o)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(o=r,r=t[++i],e<r)break e}s=t.length;break t}if(!(e>=o)){let a=t[1];e<a&&(i=2,o=a);for(let c=i-2;;){if(o===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=o,o=t[--i-1],e>=o)break e}s=i,i=0;break t}break n}for(;i<s;){let a=i+s>>>1;e<t[a]?s=a:i=a+1}if(r=t[i],o=t[i-1],o===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,o,r)}return this.interpolate_(i,o,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,o=e*r;for(let s=0;s!==r;++s)t[s]=i[o+s];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},th=class extends So{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Ey,endingEnd:Ey}}intervalChanged_(e,t,i){let r=this.parameterPositions,o=e-2,s=e+1,a=r[o],c=r[s];if(a===void 0)switch(this.getSettings_().endingStart){case My:o=e,a=2*t-i;break;case by:o=r.length-2,a=t+r[o]-r[o+1];break;default:o=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case My:s=e,c=2*i-t;break;case by:s=1,c=i+r[1]-r[0];break;default:s=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=o*u,this._offsetNext=s*u}interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,d=this._offsetNext,f=this._weightPrev,h=this._weightNext,m=(i-t)/(r-t),y=m*m,g=y*m,p=-f*g+2*f*y-f*m,E=(1+f)*g+(-1.5-2*f)*y+(-.5+f)*m+1,w=(-1-h)*g+(1.5+h)*y+.5*m,S=h*g-h*y;for(let I=0;I!==a;++I)o[I]=p*s[u+I]+E*s[l+I]+w*s[c+I]+S*s[d+I];return o}},nh=class extends So{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),d=1-u;for(let f=0;f!==a;++f)o[f]=s[l+f]*d+s[c+f]*u;return o}},ih=class extends So{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},rh=class extends So{interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,d=u.inTangents,f=u.outTangents;if(!d||!f){let y=(i-t)/(r-t),g=1-y;for(let p=0;p!==a;++p)o[p]=s[l+p]*g+s[c+p]*y;return o}let h=a*2,m=e-1;for(let y=0;y!==a;++y){let g=s[l+y],p=s[c+y],E=m*h+y*2,w=f[E],S=f[E+1],I=e*h+y*2,R=d[I],D=d[I+1],_=(i-t)/(r-t),M,q,C,U,B;for(let W=0;W<8;W++){M=_*_,q=M*_,C=1-_,U=C*C,B=U*C;let V=B*t+3*U*_*w+3*C*M*R+q*r-i;if(Math.abs(V)<1e-10)break;let O=3*U*(w-t)+6*C*_*(R-w)+3*M*(r-R);if(Math.abs(O)<1e-10)break;_=_-V/O,_=Math.max(0,Math.min(1,_))}o[y]=B*g+3*U*_*S+3*C*M*D+q*p}return o}},hi=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=Rf(t,this.TimeBufferType),this.values=Rf(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:Rf(e.times,Array),values:Rf(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new ih(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new nh(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new th(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new rh(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case bl:t=this.InterpolantFactoryMethodDiscrete;break;case Wf:t=this.InterpolantFactoryMethodLinear;break;case Pf:t=this.InterpolantFactoryMethodSmooth;break;case xy:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return Fe("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return bl;case this.InterpolantFactoryMethodLinear:return Wf;case this.InterpolantFactoryMethodSmooth:return Pf;case this.InterpolantFactoryMethodBezier:return xy}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,o=0,s=r-1;for(;o!==r&&i[o]<e;)++o;for(;s!==-1&&i[s]>t;)--s;if(++s,o!==0||s!==r){o>=s&&(s=Math.max(s,1),o=s-1);let a=this.getValueSize();this.times=i.slice(o,s),this.values=this.values.slice(o*a,s*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(ke("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,o=i.length;o===0&&(ke("KeyframeTrack: Track is empty.",this),e=!1);let s=null;for(let a=0;a!==o;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){ke("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(s!==null&&s>c){ke("KeyframeTrack: Out of order keys.",this,a,c,s),e=!1;break}s=c}if(r!==void 0&&N1(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){ke("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===Pf,o=e.length-1,s=1;for(let a=1;a<o;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let d=a*i,f=d-i,h=d+i;for(let m=0;m!==i;++m){let y=t[d+m];if(y!==t[f+m]||y!==t[h+m]){c=!0;break}}}if(c){if(a!==s){e[s]=e[a];let d=a*i,f=s*i;for(let h=0;h!==i;++h)t[f+h]=t[d+h]}++s}}if(o>0){e[s]=e[o];for(let a=o*i,c=s*i,l=0;l!==i;++l)t[c+l]=t[a+l];++s}return s!==e.length?(this.times=e.slice(0,s),this.values=t.slice(0,s*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};hi.prototype.ValueTypeName="";hi.prototype.TimeBufferType=Float32Array;hi.prototype.ValueBufferType=Float32Array;hi.prototype.DefaultInterpolation=Wf;var wo=class extends hi{constructor(e,t,i){super(e,t,i)}};wo.prototype.ValueTypeName="bool";wo.prototype.ValueBufferType=Array;wo.prototype.DefaultInterpolation=bl;wo.prototype.InterpolantFactoryMethodLinear=void 0;wo.prototype.InterpolantFactoryMethodSmooth=void 0;var oh=class extends hi{constructor(e,t,i,r){super(e,t,i,r)}};oh.prototype.ValueTypeName="color";var sh=class extends hi{constructor(e,t,i,r){super(e,t,i,r)}};sh.prototype.ValueTypeName="number";var ah=class extends So{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let o=this.resultBuffer,s=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)ui.slerpFlat(o,0,s,l-a,s,l,c);return o}},Ol=class extends hi{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new ah(this.times,this.values,this.getValueSize(),e)}};Ol.prototype.ValueTypeName="quaternion";Ol.prototype.InterpolantFactoryMethodSmooth=void 0;var To=class extends hi{constructor(e,t,i){super(e,t,i)}};To.prototype.ValueTypeName="string";To.prototype.ValueBufferType=Array;To.prototype.DefaultInterpolation=bl;To.prototype.InterpolantFactoryMethodLinear=void 0;To.prototype.InterpolantFactoryMethodSmooth=void 0;var ch=class extends hi{constructor(e,t,i,r){super(e,t,i,r)}};ch.prototype.ValueTypeName="vector";var Ga=class extends Xn{constructor(e,t=1){super(),this.isLight=!0,this.type="Light",this.color=new Qe(e),this.intensity=t}dispose(){this.dispatchEvent({type:"dispose"})}copy(e,t){return super.copy(e,t),this.color.copy(e.color),this.intensity=e.intensity,this}toJSON(e){let t=super.toJSON(e);return t.object.color=this.color.getHex(),t.object.intensity=this.intensity,t}},Fl=class extends Ga{constructor(e,t,i){super(e,i),this.isHemisphereLight=!0,this.type="HemisphereLight",this.position.copy(Xn.DEFAULT_UP),this.updateMatrix(),this.groundColor=new Qe(t)}copy(e,t){return super.copy(e,t),this.groundColor.copy(e.groundColor),this}toJSON(e){let t=super.toJSON(e);return t.object.groundColor=this.groundColor.getHex(),t}},yy=new Bt,zb=new T,Gb=new T,Ty=class{constructor(e){this.camera=e,this.intensity=1,this.bias=0,this.biasNode=null,this.normalBias=0,this.radius=1,this.blurSamples=8,this.mapSize=new Ue(512,512),this.mapType=Yn,this.map=null,this.mapPass=null,this.matrix=new Bt,this.autoUpdate=!0,this.needsUpdate=!1,this._frustum=new Va,this._frameExtents=new Ue(1,1),this._viewportCount=1,this._viewports=[new jt(0,0,1,1)]}getViewportCount(){return this._viewportCount}getFrustum(){return this._frustum}updateMatrices(e){let t=this.camera,i=this.matrix;zb.setFromMatrixPosition(e.matrixWorld),t.position.copy(zb),Gb.setFromMatrixPosition(e.target.matrixWorld),t.lookAt(Gb),t.updateMatrixWorld(),yy.multiplyMatrices(t.projectionMatrix,t.matrixWorldInverse),this._frustum.setFromProjectionMatrix(yy,t.coordinateSystem,t.reversedDepth),t.coordinateSystem===Oa||t.reversedDepth?i.set(.5,0,0,.5,0,.5,0,.5,0,0,1,0,0,0,0,1):i.set(.5,0,0,.5,0,.5,0,.5,0,0,.5,.5,0,0,0,1),i.multiply(yy)}getViewport(e){return this._viewports[e]}getFrameExtents(){return this._frameExtents}dispose(){this.map&&this.map.dispose(),this.mapPass&&this.mapPass.dispose()}copy(e){return this.camera=e.camera.clone(),this.intensity=e.intensity,this.bias=e.bias,this.radius=e.radius,this.autoUpdate=e.autoUpdate,this.needsUpdate=e.needsUpdate,this.normalBias=e.normalBias,this.blurSamples=e.blurSamples,this.mapSize.copy(e.mapSize),this.biasNode=e.biasNode,this}clone(){return new this.constructor().copy(this)}toJSON(){let e={};return this.intensity!==1&&(e.intensity=this.intensity),this.bias!==0&&(e.bias=this.bias),this.normalBias!==0&&(e.normalBias=this.normalBias),this.radius!==1&&(e.radius=this.radius),(this.mapSize.x!==512||this.mapSize.y!==512)&&(e.mapSize=this.mapSize.toArray()),e.camera=this.camera.toJSON(!1).object,delete e.camera.matrix,e}},Df=new T,Nf=new ui,cr=new T,kl=class extends Xn{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Bt,this.projectionMatrix=new Bt,this.projectionMatrixInverse=new Bt,this.coordinateSystem=$i,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(Df,Nf,cr),cr.x===1&&cr.y===1&&cr.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Df,Nf,cr.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(Df,Nf,cr),cr.x===1&&cr.y===1&&cr.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Df,Nf,cr.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},vo=new T,Wb=new Ue,jb=new Ue,On=class extends kl{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=ka*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(El*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return ka*2*Math.atan(Math.tan(El*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){vo.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(vo.x,vo.y).multiplyScalar(-e/vo.z),vo.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(vo.x,vo.y).multiplyScalar(-e/vo.z)}getViewSize(e,t){return this.getViewBounds(e,Wb,jb),t.subVectors(jb,Wb)}setViewOffset(e,t,i,r,o,s){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=o,this.view.height=s,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(El*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,o=-.5*r,s=this.view;if(this.view!==null&&this.view.enabled){let c=s.fullWidth,l=s.fullHeight;o+=s.offsetX*r/c,t-=s.offsetY*i/l,r*=s.width/c,i*=s.height/l}let a=this.filmOffset;a!==0&&(o+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(o,o+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var Wa=class extends kl{constructor(e=-1,t=1,i=1,r=-1,o=.1,s=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=o,this.far=s,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,o,s){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=o,this.view.height=s,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,o=i-e,s=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;o+=l*this.view.offsetX,s=o+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(o,s,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}},Cy=class extends Ty{constructor(){super(new Wa(-5,5,5,-5,.5,500)),this.isDirectionalLightShadow=!0}},ja=class extends Ga{constructor(e,t){super(e,t),this.isDirectionalLight=!0,this.type="DirectionalLight",this.position.copy(Xn.DEFAULT_UP),this.updateMatrix(),this.target=new Xn,this.shadow=new Cy}dispose(){super.dispose(),this.shadow.dispose()}copy(e){return super.copy(e),this.target=e.target.clone(),this.shadow=e.shadow.clone(),this}toJSON(e){let t=super.toJSON(e);return t.object.shadow=this.shadow.toJSON(),t.object.target=this.target.uuid,t}},Ul=class extends Ga{constructor(e,t){super(e,t),this.isAmbientLight=!0,this.type="AmbientLight"}};var Da=-90,Na=1,lh=class extends Xn{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new On(Da,Na,e,t);r.layers=this.layers,this.add(r);let o=new On(Da,Na,e,t);o.layers=this.layers,this.add(o);let s=new On(Da,Na,e,t);s.layers=this.layers,this.add(s);let a=new On(Da,Na,e,t);a.layers=this.layers,this.add(a);let c=new On(Da,Na,e,t);c.layers=this.layers,this.add(c);let l=new On(Da,Na,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,o,s,a,c]=t;for(let l of t)this.remove(l);if(e===$i)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),o.up.set(0,0,-1),o.lookAt(0,1,0),s.up.set(0,0,1),s.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Oa)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),o.up.set(0,0,1),o.lookAt(0,1,0),s.up.set(0,0,-1),s.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[o,s,a,c,l,u]=this.children,d=e.getRenderTarget(),f=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),m=e.xr.enabled;e.xr.enabled=!1;let y=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let g=!1;e.isWebGLRenderer===!0?g=e.state.buffers.depth.getReversed():g=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,1,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,2,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=y,e.setRenderTarget(i,5,r),g&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(d,f,h),e.xr.enabled=m,i.texture.needsPMREMUpdate=!0}},uh=class extends On{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var e_="\\[\\]\\.:\\/",gN=new RegExp("["+e_+"]","g"),t_="[^"+e_+"]",vN="[^"+e_.replace("\\.","")+"]",yN=/((?:WC+[\/:])*)/.source.replace("WC",t_),_N=/(WCOD+)?/.source.replace("WCOD",vN),xN=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",t_),EN=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",t_),MN=new RegExp("^"+yN+_N+xN+EN+"$"),bN=["material","materials","bones","map"],Iy=class{constructor(e,t,i){let r=i||Xt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,o=i.length;r!==o;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},Xt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(gN,"")}static parseTrackName(t){let i=MN.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},o=r.nodeName&&r.nodeName.lastIndexOf(".");if(o!==void 0&&o!==-1){let s=r.nodeName.substring(o+1);bN.indexOf(s)!==-1&&(r.nodeName=r.nodeName.substring(0,o),r.objectName=s)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(s){for(let a=0;a<s.length;a++){let c=s[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},o=r(t.children);if(o)return o}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)t[i++]=r[o]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)r[o]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)r[o]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let o=0,s=r.length;o!==s;++o)r[o]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,o=i.propertyName,s=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){Fe("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){ke("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){ke("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){ke("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let d=0;d<t.length;d++)if(t[d].name===u){u=d;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){ke("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){ke("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){ke("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){ke("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[o];if(a===void 0){let u=i.nodeName;ke("PropertyBinding: Trying to update property for track: "+u+"."+o+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(s!==void 0){if(o==="morphTargetInfluences"){if(!t.geometry){ke("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){ke("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[s]!==void 0&&(s=t.morphTargetDictionary[s])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=s}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=o;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Iy,n})();Xt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};Xt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};Xt.prototype.GetterByBindingType=[Xt.prototype._getValue_direct,Xt.prototype._getValue_array,Xt.prototype._getValue_arrayElement,Xt.prototype._getValue_toArray];Xt.prototype.SetterByBindingTypeAndVersioning=[[Xt.prototype._setValue_direct,Xt.prototype._setValue_direct_setNeedsUpdate,Xt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[Xt.prototype._setValue_array,Xt.prototype._setValue_array_setNeedsUpdate,Xt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[Xt.prototype._setValue_arrayElement,Xt.prototype._setValue_arrayElement_setNeedsUpdate,Xt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[Xt.prototype._setValue_fromArray,Xt.prototype._setValue_fromArray_setNeedsUpdate,Xt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var i9=new Float32Array(1);var $b=new Bt,Bl=class{constructor(e,t,i=0,r=1/0){this.ray=new Eo(e,t),this.near=i,this.far=r,this.camera=null,this.layers=new Ba,this.params={Mesh:{},Line:{threshold:1},LOD:{},Points:{threshold:1},Sprite:{}}}set(e,t){this.ray.set(e,t)}setFromCamera(e,t){t.isPerspectiveCamera?(this.ray.origin.setFromMatrixPosition(t.matrixWorld),this.ray.direction.set(e.x,e.y,.5).unproject(t).sub(this.ray.origin).normalize(),this.camera=t):t.isOrthographicCamera?(this.ray.origin.set(e.x,e.y,(t.near+t.far)/(t.near-t.far)).unproject(t),this.ray.direction.set(0,0,-1).transformDirection(t.matrixWorld),this.camera=t):ke("Raycaster: Unsupported camera type: "+t.type)}setFromXRController(e){return $b.identity().extractRotation(e.matrixWorld),this.ray.origin.setFromMatrixPosition(e.matrixWorld),this.ray.direction.set(0,0,-1).applyMatrix4($b),this}intersectObject(e,t=!0,i=[]){return Ay(e,this,i,t),i.sort(qb),i}intersectObjects(e,t=!0,i=[]){for(let r=0,o=e.length;r<o;r++)Ay(e[r],this,i,t);return i.sort(qb),i}};function qb(n,e){return n.distance-e.distance}function Ay(n,e,t,i){let r=!0;if(n.layers.test(e.layers)&&n.raycast(e,t)===!1&&(r=!1),r===!0&&i===!0){let o=n.children;for(let s=0,a=o.length;s<a;s++)Ay(o[s],e,t,!0)}}var Hl=class{constructor(e=!0){this.autoStart=e,this.startTime=0,this.oldTime=0,this.elapsedTime=0,this.running=!1,Fe("THREE.Clock: This module has been deprecated. Please use THREE.Timer instead.")}start(){this.startTime=performance.now(),this.oldTime=this.startTime,this.elapsedTime=0,this.running=!0}stop(){this.getElapsedTime(),this.running=!1,this.autoStart=!1}getElapsedTime(){return this.getDelta(),this.elapsedTime}getDelta(){let e=0;if(this.autoStart&&!this.running)return this.start(),0;if(this.running){let t=performance.now();e=(t-this.oldTime)/1e3,this.oldTime=t,this.elapsedTime+=e}return e}},$a=class{constructor(e=1,t=0,i=0){this.radius=e,this.phi=t,this.theta=i}set(e,t,i){return this.radius=e,this.phi=t,this.theta=i,this}copy(e){return this.radius=e.radius,this.phi=e.phi,this.theta=e.theta,this}makeSafe(){return this.phi=ot(this.phi,1e-6,Math.PI-1e-6),this}setFromVector3(e){return this.setFromCartesianCoords(e.x,e.y,e.z)}setFromCartesianCoords(e,t,i){return this.radius=Math.sqrt(e*e+t*t+i*i),this.radius===0?(this.theta=0,this.phi=0):(this.theta=Math.atan2(e,i),this.phi=Math.acos(ot(t/this.radius,-1,1))),this}clone(){return new this.constructor().copy(this)}};var _s=class extends dr{constructor(e,t=null){super(),this.object=e,this.domElement=t,this.enabled=!0,this.state=-1,this.keys={},this.mouseButtons={LEFT:null,MIDDLE:null,RIGHT:null},this.touches={ONE:null,TWO:null}}connect(e){if(e===void 0){Fe("Controls: connect() now requires an element.");return}this.domElement!==null&&this.disconnect(),this.domElement=e}disconnect(){}dispose(){}update(){}};function n_(n,e,t,i){let r=SN(i);switch(t){case jy:return n*e;case qy:return n*e/r.components*r.byteLength;case yh:return n*e/r.components*r.byteLength;case Es:return n*e*2/r.components*r.byteLength;case _h:return n*e*2/r.components*r.byteLength;case $y:return n*e*3/r.components*r.byteLength;case Ti:return n*e*4/r.components*r.byteLength;case xh:return n*e*4/r.components*r.byteLength;case jl:case $l:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case ql:case Xl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Mh:case Sh:return Math.max(n,16)*Math.max(e,8)/4;case Eh:case bh:return Math.max(n,8)*Math.max(e,8)/2;case wh:case Th:case Ih:case Ah:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Ch:case Rh:case Dh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Nh:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Ph:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case Lh:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case Oh:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case Fh:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case kh:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Uh:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Bh:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Hh:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Vh:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case zh:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case Gh:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case Wh:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case jh:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case $h:case qh:case Xh:return Math.ceil(n/4)*Math.ceil(e/4)*16;case Yh:case Zh:return Math.ceil(n/4)*Math.ceil(e/4)*8;case Kh:case Jh:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function SN(n){switch(n){case Yn:case Vy:return{byteLength:1,components:1};case Xa:case zy:case pr:return{byteLength:2,components:1};case gh:case vh:return{byteLength:2,components:4};case Xi:case mh:case Yi:return{byteLength:4,components:1};case Gy:case Wy:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"183"}}));typeof window<"u"&&(window.__THREE__?Fe("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="183");function QS(){let n=null,e=!1,t=null,i=null;function r(o,s){t(o,s),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(o){t=o},setContext:function(o){n=o}}}function TN(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,d=l.byteLength,f=n.createBuffer();n.bindBuffer(c,f),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:f,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:d}}function i(a,c,l){let u=c.array,d=c.updateRanges;if(n.bindBuffer(l,a),d.length===0)n.bufferSubData(l,0,u);else{d.sort((h,m)=>h.start-m.start);let f=0;for(let h=1;h<d.length;h++){let m=d[f],y=d[h];y.start<=m.start+m.count+1?m.count=Math.max(m.count,y.start+y.count-m.start):(++f,d[f]=y)}d.length=f+1;for(let h=0,m=d.length;h<m;h++){let y=d[h];n.bufferSubData(l,y.start*u.BYTES_PER_ELEMENT,u,y.start,y.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function o(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function s(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:o,update:s}}var CN=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,IN=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,AN=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,RN=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,DN=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,NN=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,PN=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,LN=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,ON=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,FN=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,kN=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,UN=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,BN=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,HN=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,VN=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,zN=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,GN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,WN=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,jN=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,$N=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,qN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,XN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,YN=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,ZN=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,KN=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,JN=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,QN=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,eP=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,tP=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,nP=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,iP="gl_FragColor = linearToOutputTexel( gl_FragColor );",rP=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,oP=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * vec3( flipEnvMap * reflectVec.x, reflectVec.yz ) );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,sP=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform float flipEnvMap;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,aP=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,cP=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,lP=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,uP=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,dP=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,fP=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,hP=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,pP=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,mP=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,gP=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,vP=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,yP=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif`,_P=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,xP=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,EP=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,MP=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,bP=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,SP=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,wP=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		float v = 0.5 / ( gv + gl );
		return v;
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,TP=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,CP=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,IP=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,AP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,RP=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,DP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,NP=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,PP=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,LP=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,OP=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,FP=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,kP=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,UP=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,BP=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,HP=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,VP=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,zP=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,GP=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,WP=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,jP=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,$P=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,qP=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,XP=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,YP=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,ZP=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,KP=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,JP=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,QP=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,eL=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,tL=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,nL=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,iL=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,rL=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,oL=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,sL=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,aL=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,cL=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,lL=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,uL=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,dL=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,fL=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,hL=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,pL=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,mL=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,gL=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,vL=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,yL=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,_L=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,xL=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,EL=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,ML=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,bL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,SL=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,wL=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,TL=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,CL=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,IL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,AL=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float flipEnvMap;
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vec3( flipEnvMap * vWorldDirection.x, vWorldDirection.yz ) );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,RL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,DL=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,NL=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,PL=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,LL=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,OL=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,FL=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,kL=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,UL=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,BL=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,HL=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,VL=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,zL=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,GL=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,WL=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,jL=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,$L=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,qL=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,XL=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,YL=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,ZL=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,KL=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,JL=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,QL=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,eO=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,tO=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,nO=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,iO=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,rO=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,oO=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,nt={alphahash_fragment:CN,alphahash_pars_fragment:IN,alphamap_fragment:AN,alphamap_pars_fragment:RN,alphatest_fragment:DN,alphatest_pars_fragment:NN,aomap_fragment:PN,aomap_pars_fragment:LN,batching_pars_vertex:ON,batching_vertex:FN,begin_vertex:kN,beginnormal_vertex:UN,bsdfs:BN,iridescence_fragment:HN,bumpmap_pars_fragment:VN,clipping_planes_fragment:zN,clipping_planes_pars_fragment:GN,clipping_planes_pars_vertex:WN,clipping_planes_vertex:jN,color_fragment:$N,color_pars_fragment:qN,color_pars_vertex:XN,color_vertex:YN,common:ZN,cube_uv_reflection_fragment:KN,defaultnormal_vertex:JN,displacementmap_pars_vertex:QN,displacementmap_vertex:eP,emissivemap_fragment:tP,emissivemap_pars_fragment:nP,colorspace_fragment:iP,colorspace_pars_fragment:rP,envmap_fragment:oP,envmap_common_pars_fragment:sP,envmap_pars_fragment:aP,envmap_pars_vertex:cP,envmap_physical_pars_fragment:_P,envmap_vertex:lP,fog_vertex:uP,fog_pars_vertex:dP,fog_fragment:fP,fog_pars_fragment:hP,gradientmap_pars_fragment:pP,lightmap_pars_fragment:mP,lights_lambert_fragment:gP,lights_lambert_pars_fragment:vP,lights_pars_begin:yP,lights_toon_fragment:xP,lights_toon_pars_fragment:EP,lights_phong_fragment:MP,lights_phong_pars_fragment:bP,lights_physical_fragment:SP,lights_physical_pars_fragment:wP,lights_fragment_begin:TP,lights_fragment_maps:CP,lights_fragment_end:IP,logdepthbuf_fragment:AP,logdepthbuf_pars_fragment:RP,logdepthbuf_pars_vertex:DP,logdepthbuf_vertex:NP,map_fragment:PP,map_pars_fragment:LP,map_particle_fragment:OP,map_particle_pars_fragment:FP,metalnessmap_fragment:kP,metalnessmap_pars_fragment:UP,morphinstance_vertex:BP,morphcolor_vertex:HP,morphnormal_vertex:VP,morphtarget_pars_vertex:zP,morphtarget_vertex:GP,normal_fragment_begin:WP,normal_fragment_maps:jP,normal_pars_fragment:$P,normal_pars_vertex:qP,normal_vertex:XP,normalmap_pars_fragment:YP,clearcoat_normal_fragment_begin:ZP,clearcoat_normal_fragment_maps:KP,clearcoat_pars_fragment:JP,iridescence_pars_fragment:QP,opaque_fragment:eL,packing:tL,premultiplied_alpha_fragment:nL,project_vertex:iL,dithering_fragment:rL,dithering_pars_fragment:oL,roughnessmap_fragment:sL,roughnessmap_pars_fragment:aL,shadowmap_pars_fragment:cL,shadowmap_pars_vertex:lL,shadowmap_vertex:uL,shadowmask_pars_fragment:dL,skinbase_vertex:fL,skinning_pars_vertex:hL,skinning_vertex:pL,skinnormal_vertex:mL,specularmap_fragment:gL,specularmap_pars_fragment:vL,tonemapping_fragment:yL,tonemapping_pars_fragment:_L,transmission_fragment:xL,transmission_pars_fragment:EL,uv_pars_fragment:ML,uv_pars_vertex:bL,uv_vertex:SL,worldpos_vertex:wL,background_vert:TL,background_frag:CL,backgroundCube_vert:IL,backgroundCube_frag:AL,cube_vert:RL,cube_frag:DL,depth_vert:NL,depth_frag:PL,distance_vert:LL,distance_frag:OL,equirect_vert:FL,equirect_frag:kL,linedashed_vert:UL,linedashed_frag:BL,meshbasic_vert:HL,meshbasic_frag:VL,meshlambert_vert:zL,meshlambert_frag:GL,meshmatcap_vert:WL,meshmatcap_frag:jL,meshnormal_vert:$L,meshnormal_frag:qL,meshphong_vert:XL,meshphong_frag:YL,meshphysical_vert:ZL,meshphysical_frag:KL,meshtoon_vert:JL,meshtoon_frag:QL,points_vert:eO,points_frag:tO,shadow_vert:nO,shadow_frag:iO,sprite_vert:rO,sprite_frag:oO},de={common:{diffuse:{value:new Qe(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new et},alphaMap:{value:null},alphaMapTransform:{value:new et},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new et}},envmap:{envMap:{value:null},envMapRotation:{value:new et},flipEnvMap:{value:-1},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new et}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new et}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new et},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new et},normalScale:{value:new Ue(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new et},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new et}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new et}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new et}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Qe(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null}},points:{diffuse:{value:new Qe(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new et},alphaTest:{value:0},uvTransform:{value:new et}},sprite:{diffuse:{value:new Qe(16777215)},opacity:{value:1},center:{value:new Ue(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new et},alphaMap:{value:null},alphaMapTransform:{value:new et},alphaTest:{value:0}}},vr={basic:{uniforms:Fn([de.common,de.specularmap,de.envmap,de.aomap,de.lightmap,de.fog]),vertexShader:nt.meshbasic_vert,fragmentShader:nt.meshbasic_frag},lambert:{uniforms:Fn([de.common,de.specularmap,de.envmap,de.aomap,de.lightmap,de.emissivemap,de.bumpmap,de.normalmap,de.displacementmap,de.fog,de.lights,{emissive:{value:new Qe(0)},envMapIntensity:{value:1}}]),vertexShader:nt.meshlambert_vert,fragmentShader:nt.meshlambert_frag},phong:{uniforms:Fn([de.common,de.specularmap,de.envmap,de.aomap,de.lightmap,de.emissivemap,de.bumpmap,de.normalmap,de.displacementmap,de.fog,de.lights,{emissive:{value:new Qe(0)},specular:{value:new Qe(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:nt.meshphong_vert,fragmentShader:nt.meshphong_frag},standard:{uniforms:Fn([de.common,de.envmap,de.aomap,de.lightmap,de.emissivemap,de.bumpmap,de.normalmap,de.displacementmap,de.roughnessmap,de.metalnessmap,de.fog,de.lights,{emissive:{value:new Qe(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:nt.meshphysical_vert,fragmentShader:nt.meshphysical_frag},toon:{uniforms:Fn([de.common,de.aomap,de.lightmap,de.emissivemap,de.bumpmap,de.normalmap,de.displacementmap,de.gradientmap,de.fog,de.lights,{emissive:{value:new Qe(0)}}]),vertexShader:nt.meshtoon_vert,fragmentShader:nt.meshtoon_frag},matcap:{uniforms:Fn([de.common,de.bumpmap,de.normalmap,de.displacementmap,de.fog,{matcap:{value:null}}]),vertexShader:nt.meshmatcap_vert,fragmentShader:nt.meshmatcap_frag},points:{uniforms:Fn([de.points,de.fog]),vertexShader:nt.points_vert,fragmentShader:nt.points_frag},dashed:{uniforms:Fn([de.common,de.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:nt.linedashed_vert,fragmentShader:nt.linedashed_frag},depth:{uniforms:Fn([de.common,de.displacementmap]),vertexShader:nt.depth_vert,fragmentShader:nt.depth_frag},normal:{uniforms:Fn([de.common,de.bumpmap,de.normalmap,de.displacementmap,{opacity:{value:1}}]),vertexShader:nt.meshnormal_vert,fragmentShader:nt.meshnormal_frag},sprite:{uniforms:Fn([de.sprite,de.fog]),vertexShader:nt.sprite_vert,fragmentShader:nt.sprite_frag},background:{uniforms:{uvTransform:{value:new et},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:nt.background_vert,fragmentShader:nt.background_frag},backgroundCube:{uniforms:{envMap:{value:null},flipEnvMap:{value:-1},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new et}},vertexShader:nt.backgroundCube_vert,fragmentShader:nt.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:nt.cube_vert,fragmentShader:nt.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:nt.equirect_vert,fragmentShader:nt.equirect_frag},distance:{uniforms:Fn([de.common,de.displacementmap,{referencePosition:{value:new T},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:nt.distance_vert,fragmentShader:nt.distance_frag},shadow:{uniforms:Fn([de.lights,de.fog,{color:{value:new Qe(0)},opacity:{value:1}}]),vertexShader:nt.shadow_vert,fragmentShader:nt.shadow_frag}};vr.physical={uniforms:Fn([vr.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new et},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new et},clearcoatNormalScale:{value:new Ue(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new et},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new et},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new et},sheen:{value:0},sheenColor:{value:new Qe(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new et},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new et},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new et},transmissionSamplerSize:{value:new Ue},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new et},attenuationDistance:{value:0},attenuationColor:{value:new Qe(0)},specularColor:{value:new Qe(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new et},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new et},anisotropyVector:{value:new Ue},anisotropyMap:{value:null},anisotropyMapTransform:{value:new et}}]),vertexShader:nt.meshphysical_vert,fragmentShader:nt.meshphysical_frag};var tp={r:0,b:0,g:0},bs=new fr,sO=new Bt;function aO(n,e,t,i,r,o){let s=new Qe(0),a=r===!0?0:1,c,l,u=null,d=0,f=null;function h(E){let w=E.isScene===!0?E.background:null;if(w&&w.isTexture){let S=E.backgroundBlurriness>0;w=e.get(w,S)}return w}function m(E){let w=!1,S=h(E);S===null?g(s,a):S&&S.isColor&&(g(S,1),w=!0);let I=n.xr.getEnvironmentBlendMode();I==="additive"?t.buffers.color.setClear(0,0,0,1,o):I==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,o),(n.autoClear||w)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function y(E,w){let S=h(w);S&&(S.isCubeTexture||S.mapping===Gl)?(l===void 0&&(l=new Me(new ct(1,1,1),new fi({name:"BackgroundCubeMaterial",uniforms:Ms(vr.backgroundCube.uniforms),vertexShader:vr.backgroundCube.vertexShader,fragmentShader:vr.backgroundCube.fragmentShader,side:zn,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(I,R,D){this.matrixWorld.copyPosition(D.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),bs.copy(w.backgroundRotation),bs.x*=-1,bs.y*=-1,bs.z*=-1,S.isCubeTexture&&S.isRenderTargetTexture===!1&&(bs.y*=-1,bs.z*=-1),l.material.uniforms.envMap.value=S,l.material.uniforms.flipEnvMap.value=S.isCubeTexture&&S.isRenderTargetTexture===!1?-1:1,l.material.uniforms.backgroundBlurriness.value=w.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(sO.makeRotationFromEuler(bs)),l.material.toneMapped=mt.getTransfer(S.colorSpace)!==At,(u!==S||d!==S.version||f!==n.toneMapping)&&(l.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),l.layers.enableAll(),E.unshift(l,l.geometry,l.material,0,0,null)):S&&S.isTexture&&(c===void 0&&(c=new Me(new Wt(2,2),new fi({name:"BackgroundMaterial",uniforms:Ms(vr.background.uniforms),vertexShader:vr.background.vertexShader,fragmentShader:vr.background.fragmentShader,side:Or,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=S,c.material.uniforms.backgroundIntensity.value=w.backgroundIntensity,c.material.toneMapped=mt.getTransfer(S.colorSpace)!==At,S.matrixAutoUpdate===!0&&S.updateMatrix(),c.material.uniforms.uvTransform.value.copy(S.matrix),(u!==S||d!==S.version||f!==n.toneMapping)&&(c.material.needsUpdate=!0,u=S,d=S.version,f=n.toneMapping),c.layers.enableAll(),E.unshift(c,c.geometry,c.material,0,0,null))}function g(E,w){E.getRGB(tp,Qy(n)),t.buffers.color.setClear(tp.r,tp.g,tp.b,w,o)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return s},setClearColor:function(E,w=1){s.set(E),a=w,g(s,a)},getClearAlpha:function(){return a},setClearAlpha:function(E){a=E,g(s,a)},render:m,addToRenderList:y,dispose:p}}function cO(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=f(null),o=r,s=!1;function a(C,U,B,W,H){let V=!1,O=d(C,W,B,U);o!==O&&(o=O,l(o.object)),V=h(C,W,B,H),V&&m(C,W,B,H),H!==null&&e.update(H,n.ELEMENT_ARRAY_BUFFER),(V||s)&&(s=!1,S(C,U,B,W),H!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(H).buffer))}function c(){return n.createVertexArray()}function l(C){return n.bindVertexArray(C)}function u(C){return n.deleteVertexArray(C)}function d(C,U,B,W){let H=W.wireframe===!0,V=i[U.id];V===void 0&&(V={},i[U.id]=V);let O=C.isInstancedMesh===!0?C.id:0,ee=V[O];ee===void 0&&(ee={},V[O]=ee);let Z=ee[B.id];Z===void 0&&(Z={},ee[B.id]=Z);let fe=Z[H];return fe===void 0&&(fe=f(c()),Z[H]=fe),fe}function f(C){let U=[],B=[],W=[];for(let H=0;H<t;H++)U[H]=0,B[H]=0,W[H]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:U,enabledAttributes:B,attributeDivisors:W,object:C,attributes:{},index:null}}function h(C,U,B,W){let H=o.attributes,V=U.attributes,O=0,ee=B.getAttributes();for(let Z in ee)if(ee[Z].location>=0){let ye=H[Z],pe=V[Z];if(pe===void 0&&(Z==="instanceMatrix"&&C.instanceMatrix&&(pe=C.instanceMatrix),Z==="instanceColor"&&C.instanceColor&&(pe=C.instanceColor)),ye===void 0||ye.attribute!==pe||pe&&ye.data!==pe.data)return!0;O++}return o.attributesNum!==O||o.index!==W}function m(C,U,B,W){let H={},V=U.attributes,O=0,ee=B.getAttributes();for(let Z in ee)if(ee[Z].location>=0){let ye=V[Z];ye===void 0&&(Z==="instanceMatrix"&&C.instanceMatrix&&(ye=C.instanceMatrix),Z==="instanceColor"&&C.instanceColor&&(ye=C.instanceColor));let pe={};pe.attribute=ye,ye&&ye.data&&(pe.data=ye.data),H[Z]=pe,O++}o.attributes=H,o.attributesNum=O,o.index=W}function y(){let C=o.newAttributes;for(let U=0,B=C.length;U<B;U++)C[U]=0}function g(C){p(C,0)}function p(C,U){let B=o.newAttributes,W=o.enabledAttributes,H=o.attributeDivisors;B[C]=1,W[C]===0&&(n.enableVertexAttribArray(C),W[C]=1),H[C]!==U&&(n.vertexAttribDivisor(C,U),H[C]=U)}function E(){let C=o.newAttributes,U=o.enabledAttributes;for(let B=0,W=U.length;B<W;B++)U[B]!==C[B]&&(n.disableVertexAttribArray(B),U[B]=0)}function w(C,U,B,W,H,V,O){O===!0?n.vertexAttribIPointer(C,U,B,H,V):n.vertexAttribPointer(C,U,B,W,H,V)}function S(C,U,B,W){y();let H=W.attributes,V=B.getAttributes(),O=U.defaultAttributeValues;for(let ee in V){let Z=V[ee];if(Z.location>=0){let fe=H[ee];if(fe===void 0&&(ee==="instanceMatrix"&&C.instanceMatrix&&(fe=C.instanceMatrix),ee==="instanceColor"&&C.instanceColor&&(fe=C.instanceColor)),fe!==void 0){let ye=fe.normalized,pe=fe.itemSize,We=e.get(fe);if(We===void 0)continue;let Dt=We.buffer,wt=We.type,X=We.bytesPerElement,oe=wt===n.INT||wt===n.UNSIGNED_INT||fe.gpuType===mh;if(fe.isInterleavedBufferAttribute){let ae=fe.data,Xe=ae.stride,Le=fe.offset;if(ae.isInstancedInterleavedBuffer){for(let Ge=0;Ge<Z.locationSize;Ge++)p(Z.location+Ge,ae.meshPerAttribute);C.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=ae.meshPerAttribute*ae.count)}else for(let Ge=0;Ge<Z.locationSize;Ge++)g(Z.location+Ge);n.bindBuffer(n.ARRAY_BUFFER,Dt);for(let Ge=0;Ge<Z.locationSize;Ge++)w(Z.location+Ge,pe/Z.locationSize,wt,ye,Xe*X,(Le+pe/Z.locationSize*Ge)*X,oe)}else{if(fe.isInstancedBufferAttribute){for(let ae=0;ae<Z.locationSize;ae++)p(Z.location+ae,fe.meshPerAttribute);C.isInstancedMesh!==!0&&W._maxInstanceCount===void 0&&(W._maxInstanceCount=fe.meshPerAttribute*fe.count)}else for(let ae=0;ae<Z.locationSize;ae++)g(Z.location+ae);n.bindBuffer(n.ARRAY_BUFFER,Dt);for(let ae=0;ae<Z.locationSize;ae++)w(Z.location+ae,pe/Z.locationSize,wt,ye,pe*X,pe/Z.locationSize*ae*X,oe)}}else if(O!==void 0){let ye=O[ee];if(ye!==void 0)switch(ye.length){case 2:n.vertexAttrib2fv(Z.location,ye);break;case 3:n.vertexAttrib3fv(Z.location,ye);break;case 4:n.vertexAttrib4fv(Z.location,ye);break;default:n.vertexAttrib1fv(Z.location,ye)}}}}E()}function I(){M();for(let C in i){let U=i[C];for(let B in U){let W=U[B];for(let H in W){let V=W[H];for(let O in V)u(V[O].object),delete V[O];delete W[H]}}delete i[C]}}function R(C){if(i[C.id]===void 0)return;let U=i[C.id];for(let B in U){let W=U[B];for(let H in W){let V=W[H];for(let O in V)u(V[O].object),delete V[O];delete W[H]}}delete i[C.id]}function D(C){for(let U in i){let B=i[U];for(let W in B){let H=B[W];if(H[C.id]===void 0)continue;let V=H[C.id];for(let O in V)u(V[O].object),delete V[O];delete H[C.id]}}}function _(C){for(let U in i){let B=i[U],W=C.isInstancedMesh===!0?C.id:0,H=B[W];if(H!==void 0){for(let V in H){let O=H[V];for(let ee in O)u(O[ee].object),delete O[ee];delete H[V]}delete B[W],Object.keys(B).length===0&&delete i[U]}}}function M(){q(),s=!0,o!==r&&(o=r,l(o.object))}function q(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:M,resetDefaultState:q,dispose:I,releaseStatesOfGeometry:R,releaseStatesOfObject:_,releaseStatesOfProgram:D,initAttributes:y,enableAttribute:g,disableUnusedAttributes:E}}function lO(n,e,t){let i;function r(l){i=l}function o(l,u){n.drawArrays(i,l,u),t.update(u,i,1)}function s(l,u,d){d!==0&&(n.drawArraysInstanced(i,l,u,d),t.update(u,i,d))}function a(l,u,d){if(d===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,l,0,u,0,d);let h=0;for(let m=0;m<d;m++)h+=u[m];t.update(h,i,1)}function c(l,u,d,f){if(d===0)return;let h=e.get("WEBGL_multi_draw");if(h===null)for(let m=0;m<l.length;m++)s(l[m],u[m],f[m]);else{h.multiDrawArraysInstancedWEBGL(i,l,0,u,0,f,0,d);let m=0;for(let y=0;y<d;y++)m+=u[y]*f[y];t.update(m,i,1)}}this.setMode=r,this.render=o,this.renderInstances=s,this.renderMultiDraw=a,this.renderMultiDrawInstances=c}function uO(n,e,t,i){let r;function o(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let D=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(D.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function s(D){return!(D!==Ti&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(D){let _=D===pr&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(D!==Yn&&i.convert(D)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&D!==Yi&&!_)}function c(D){if(D==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";D="mediump"}return D==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(Fe("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let d=t.logarithmicDepthBuffer===!0,f=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control"),h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),m=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),y=n.getParameter(n.MAX_TEXTURE_SIZE),g=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),E=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),w=n.getParameter(n.MAX_VARYING_VECTORS),S=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),I=n.getParameter(n.MAX_SAMPLES),R=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:o,getMaxPrecision:c,textureFormatReadable:s,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:d,reversedDepthBuffer:f,maxTextures:h,maxVertexTextures:m,maxTextureSize:y,maxCubemapSize:g,maxAttributes:p,maxVertexUniforms:E,maxVaryings:w,maxFragmentUniforms:S,maxSamples:I,samples:R}}function dO(n){let e=this,t=null,i=0,r=!1,o=!1,s=new wi,a=new et,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(d,f){let h=d.length!==0||f||i!==0||r;return r=f,i=d.length,h},this.beginShadows=function(){o=!0,u(null)},this.endShadows=function(){o=!1},this.setGlobalState=function(d,f){t=u(d,f,0)},this.setState=function(d,f,h){let m=d.clippingPlanes,y=d.clipIntersection,g=d.clipShadows,p=n.get(d);if(!r||m===null||m.length===0||o&&!g)o?u(null):l();else{let E=o?0:i,w=E*4,S=p.clippingState||null;c.value=S,S=u(m,f,w,h);for(let I=0;I!==w;++I)S[I]=t[I];p.clippingState=S,this.numIntersection=y?this.numPlanes:0,this.numPlanes+=E}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(d,f,h,m){let y=d!==null?d.length:0,g=null;if(y!==0){if(g=c.value,m!==!0||g===null){let p=h+y*4,E=f.matrixWorldInverse;a.getNormalMatrix(E),(g===null||g.length<p)&&(g=new Float32Array(p));for(let w=0,S=h;w!==y;++w,S+=4)s.copy(d[w]).applyMatrix4(E,a),s.normal.toArray(g,S),g[S+3]=s.constant}c.value=g,c.needsUpdate=!0}return e.numPlanes=y,e.numIntersection=0,g}}var No=4,DS=[.125,.215,.35,.446,.526,.582],ws=20,fO=256,Yl=new Wa,NS=new Qe,i_=null,r_=0,o_=0,s_=!1,hO=new T,ip=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,o={}){let{size:s=256,position:a=hO}=o;i_=this._renderer.getRenderTarget(),r_=this._renderer.getActiveCubeFace(),o_=this._renderer.getActiveMipmapLevel(),s_=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(s);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=OS(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=LS(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(i_,r_,o_),this._renderer.xr.enabled=s_,e.scissorTest=!1,Ka(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===Ao||e.mapping===xs?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),i_=this._renderer.getRenderTarget(),r_=this._renderer.getActiveCubeFace(),o_=this._renderer.getActiveMipmapLevel(),s_=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:Cn,minFilter:Cn,generateMipmaps:!1,type:pr,format:Ti,colorSpace:ms,depthBuffer:!1},r=PS(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=PS(e,t,i);let{_lodMax:o}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=pO(o)),this._blurMaterial=gO(o,e,t),this._ggxMaterial=mO(o,e,t)}return r}_compileMaterial(e){let t=new Me(new _n,e);this._renderer.compile(t,Yl)}_sceneToCubeUV(e,t,i,r,o){let c=new On(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],d=this._renderer,f=d.autoClear,h=d.toneMapping;d.getClearColor(NS),d.toneMapping=qi,d.autoClear=!1,d.state.buffers.depth.getReversed()&&(d.setRenderTarget(r),d.clearDepth(),d.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Me(new ct,new vs({name:"PMREM.Background",side:zn,depthWrite:!1,depthTest:!1})));let y=this._backgroundBox,g=y.material,p=!1,E=e.background;E?E.isColor&&(g.color.copy(E),e.background=null,p=!0):(g.color.copy(NS),p=!0);for(let w=0;w<6;w++){let S=w%3;S===0?(c.up.set(0,l[w],0),c.position.set(o.x,o.y,o.z),c.lookAt(o.x+u[w],o.y,o.z)):S===1?(c.up.set(0,0,l[w]),c.position.set(o.x,o.y,o.z),c.lookAt(o.x,o.y+u[w],o.z)):(c.up.set(0,l[w],0),c.position.set(o.x,o.y,o.z),c.lookAt(o.x,o.y,o.z+u[w]));let I=this._cubeSize;Ka(r,S*I,w>2?I:0,I,I),d.setRenderTarget(r),p&&d.render(y,c),d.render(e,c)}d.toneMapping=h,d.autoClear=f,e.background=E}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===Ao||e.mapping===xs;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=OS()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=LS());let o=r?this._cubemapMaterial:this._equirectMaterial,s=this._lodMeshes[0];s.material=o;let a=o.uniforms;a.envMap.value=e;let c=this._cubeSize;Ka(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(s,Yl)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let o=1;o<r;o++)this._applyGGXFilter(e,o-1,o);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,o=this._pingPongRenderTarget,s=this._ggxMaterial,a=this._lodMeshes[i];a.material=s;let c=s.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),d=Math.sqrt(l*l-u*u),f=0+l*1.25,h=d*f,{_lodMax:m}=this,y=this._sizeLods[i],g=3*y*(i>m-No?i-m+No:0),p=4*(this._cubeSize-y);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=m-t,Ka(o,g,p,3*y,2*y),r.setRenderTarget(o),r.render(a,Yl),c.envMap.value=o.texture,c.roughness.value=0,c.mipInt.value=m-i,Ka(e,g,p,3*y,2*y),r.setRenderTarget(e),r.render(a,Yl)}_blur(e,t,i,r,o){let s=this._pingPongRenderTarget;this._halfBlur(e,s,t,i,r,"latitudinal",o),this._halfBlur(s,e,i,i,r,"longitudinal",o)}_halfBlur(e,t,i,r,o,s,a){let c=this._renderer,l=this._blurMaterial;s!=="latitudinal"&&s!=="longitudinal"&&ke("blur direction must be either latitudinal or longitudinal!");let u=3,d=this._lodMeshes[r];d.material=l;let f=l.uniforms,h=this._sizeLods[i]-1,m=isFinite(o)?Math.PI/(2*h):2*Math.PI/(2*ws-1),y=o/m,g=isFinite(o)?1+Math.floor(u*y):ws;g>ws&&Fe(`sigmaRadians, ${o}, is too large and will clip, as it requested ${g} samples when the maximum is set to ${ws}`);let p=[],E=0;for(let D=0;D<ws;++D){let _=D/y,M=Math.exp(-_*_/2);p.push(M),D===0?E+=M:D<g&&(E+=2*M)}for(let D=0;D<p.length;D++)p[D]=p[D]/E;f.envMap.value=e.texture,f.samples.value=g,f.weights.value=p,f.latitudinal.value=s==="latitudinal",a&&(f.poleAxis.value=a);let{_lodMax:w}=this;f.dTheta.value=m,f.mipInt.value=w-i;let S=this._sizeLods[r],I=3*S*(r>w-No?r-w+No:0),R=4*(this._cubeSize-S);Ka(t,I,R,3*S,2*S),c.setRenderTarget(t),c.render(d,Yl)}};function pO(n){let e=[],t=[],i=[],r=n,o=n-No+1+DS.length;for(let s=0;s<o;s++){let a=Math.pow(2,r);e.push(a);let c=1/a;s>n-No?c=DS[s-n+No-1]:s===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,d=1+l,f=[u,u,d,u,d,d,u,u,d,d,u,d],h=6,m=6,y=3,g=2,p=1,E=new Float32Array(y*m*h),w=new Float32Array(g*m*h),S=new Float32Array(p*m*h);for(let R=0;R<h;R++){let D=R%3*2/3-1,_=R>2?0:-1,M=[D,_,0,D+2/3,_,0,D+2/3,_+1,0,D,_,0,D+2/3,_+1,0,D,_+1,0];E.set(M,y*m*R),w.set(f,g*m*R);let q=[R,R,R,R,R,R];S.set(q,p*m*R)}let I=new _n;I.setAttribute("position",new vn(E,y)),I.setAttribute("uv",new vn(w,g)),I.setAttribute("faceIndex",new vn(S,p)),i.push(new Me(I,null)),r>No&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function PS(n,e,t){let i=new di(n,e,t);return i.texture.mapping=Gl,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Ka(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function mO(n,e,t){return new fi({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:fO,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:sp(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:hr,depthTest:!1,depthWrite:!1})}function gO(n,e,t){let i=new Float32Array(ws),r=new T(0,1,0);return new fi({name:"SphericalGaussianBlur",defines:{n:ws,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:sp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:hr,depthTest:!1,depthWrite:!1})}function LS(){return new fi({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:sp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:hr,depthTest:!1,depthWrite:!1})}function OS(){return new fi({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:sp(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:hr,depthTest:!1,depthWrite:!1})}function sp(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var rp=class extends di{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Pl(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new ct(5,5,5),o=new fi({name:"CubemapFromEquirect",uniforms:Ms(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:zn,blending:hr});o.uniforms.tEquirect.value=t;let s=new Me(r,o),a=t.minFilter;return t.minFilter===Ro&&(t.minFilter=Cn),new lh(1,10,this).update(e,s),t.minFilter=a,s.geometry.dispose(),s.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let o=e.getRenderTarget();for(let s=0;s<6;s++)e.setRenderTarget(this,s),e.clear(t,i,r);e.setRenderTarget(o)}};function vO(n){let e=new WeakMap,t=new WeakMap,i=null;function r(f,h=!1){return f==null?null:h?s(f):o(f)}function o(f){if(f&&f.isTexture){let h=f.mapping;if(h===fh||h===hh)if(e.has(f)){let m=e.get(f).texture;return a(m,f.mapping)}else{let m=f.image;if(m&&m.height>0){let y=new rp(m.height);return y.fromEquirectangularTexture(n,f),e.set(f,y),f.addEventListener("dispose",l),a(y.texture,f.mapping)}else return null}}return f}function s(f){if(f&&f.isTexture){let h=f.mapping,m=h===fh||h===hh,y=h===Ao||h===xs;if(m||y){let g=t.get(f),p=g!==void 0?g.texture.pmremVersion:0;if(f.isRenderTargetTexture&&f.pmremVersion!==p)return i===null&&(i=new ip(n)),g=m?i.fromEquirectangular(f,g):i.fromCubemap(f,g),g.texture.pmremVersion=f.pmremVersion,t.set(f,g),g.texture;if(g!==void 0)return g.texture;{let E=f.image;return m&&E&&E.height>0||y&&E&&c(E)?(i===null&&(i=new ip(n)),g=m?i.fromEquirectangular(f):i.fromCubemap(f),g.texture.pmremVersion=f.pmremVersion,t.set(f,g),f.addEventListener("dispose",u),g.texture):null}}}return f}function a(f,h){return h===fh?f.mapping=Ao:h===hh&&(f.mapping=xs),f}function c(f){let h=0,m=6;for(let y=0;y<m;y++)f[y]!==void 0&&h++;return h===m}function l(f){let h=f.target;h.removeEventListener("dispose",l);let m=e.get(h);m!==void 0&&(e.delete(h),m.dispose())}function u(f){let h=f.target;h.removeEventListener("dispose",u);let m=t.get(h);m!==void 0&&(t.delete(h),m.dispose())}function d(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:d}}function yO(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&Tl("WebGLRenderer: "+i+" extension not supported."),r}}}function _O(n,e,t,i){let r={},o=new WeakMap;function s(d){let f=d.target;f.index!==null&&e.remove(f.index);for(let m in f.attributes)e.remove(f.attributes[m]);f.removeEventListener("dispose",s),delete r[f.id];let h=o.get(f);h&&(e.remove(h),o.delete(f)),i.releaseStatesOfGeometry(f),f.isInstancedBufferGeometry===!0&&delete f._maxInstanceCount,t.memory.geometries--}function a(d,f){return r[f.id]===!0||(f.addEventListener("dispose",s),r[f.id]=!0,t.memory.geometries++),f}function c(d){let f=d.attributes;for(let h in f)e.update(f[h],n.ARRAY_BUFFER)}function l(d){let f=[],h=d.index,m=d.attributes.position,y=0;if(m===void 0)return;if(h!==null){let E=h.array;y=h.version;for(let w=0,S=E.length;w<S;w+=3){let I=E[w+0],R=E[w+1],D=E[w+2];f.push(I,R,R,D,D,I)}}else{let E=m.array;y=m.version;for(let w=0,S=E.length/3-1;w<S;w+=3){let I=w+0,R=w+1,D=w+2;f.push(I,R,R,D,D,I)}}let g=new(m.count>=65535?Dl:Rl)(f,1);g.version=y;let p=o.get(d);p&&e.remove(p),o.set(d,g)}function u(d){let f=o.get(d);if(f){let h=d.index;h!==null&&f.version<h.version&&l(d)}else l(d);return o.get(d)}return{get:a,update:c,getWireframeAttribute:u}}function xO(n,e,t){let i;function r(f){i=f}let o,s;function a(f){o=f.type,s=f.bytesPerElement}function c(f,h){n.drawElements(i,h,o,f*s),t.update(h,i,1)}function l(f,h,m){m!==0&&(n.drawElementsInstanced(i,h,o,f*s,m),t.update(h,i,m))}function u(f,h,m){if(m===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,h,0,o,f,0,m);let g=0;for(let p=0;p<m;p++)g+=h[p];t.update(g,i,1)}function d(f,h,m,y){if(m===0)return;let g=e.get("WEBGL_multi_draw");if(g===null)for(let p=0;p<f.length;p++)l(f[p]/s,h[p],y[p]);else{g.multiDrawElementsInstancedWEBGL(i,h,0,o,f,0,y,0,m);let p=0;for(let E=0;E<m;E++)p+=h[E]*y[E];t.update(p,i,1)}}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u,this.renderMultiDrawInstances=d}function EO(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(o,s,a){switch(t.calls++,s){case n.TRIANGLES:t.triangles+=a*(o/3);break;case n.LINES:t.lines+=a*(o/2);break;case n.LINE_STRIP:t.lines+=a*(o-1);break;case n.LINE_LOOP:t.lines+=a*o;break;case n.POINTS:t.points+=a*o;break;default:ke("WebGLInfo: Unknown draw mode:",s);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function MO(n,e,t){let i=new WeakMap,r=new jt;function o(s,a,c){let l=s.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,d=u!==void 0?u.length:0,f=i.get(a);if(f===void 0||f.count!==d){let q=function(){_.dispose(),i.delete(a),a.removeEventListener("dispose",q)};var h=q;f!==void 0&&f.texture.dispose();let m=a.morphAttributes.position!==void 0,y=a.morphAttributes.normal!==void 0,g=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],E=a.morphAttributes.normal||[],w=a.morphAttributes.color||[],S=0;m===!0&&(S=1),y===!0&&(S=2),g===!0&&(S=3);let I=a.attributes.position.count*S,R=1;I>e.maxTextureSize&&(R=Math.ceil(I/e.maxTextureSize),I=e.maxTextureSize);let D=new Float32Array(I*R*4*d),_=new Cl(D,I,R,d);_.type=Yi,_.needsUpdate=!0;let M=S*4;for(let C=0;C<d;C++){let U=p[C],B=E[C],W=w[C],H=I*R*4*C;for(let V=0;V<U.count;V++){let O=V*M;m===!0&&(r.fromBufferAttribute(U,V),D[H+O+0]=r.x,D[H+O+1]=r.y,D[H+O+2]=r.z,D[H+O+3]=0),y===!0&&(r.fromBufferAttribute(B,V),D[H+O+4]=r.x,D[H+O+5]=r.y,D[H+O+6]=r.z,D[H+O+7]=0),g===!0&&(r.fromBufferAttribute(W,V),D[H+O+8]=r.x,D[H+O+9]=r.y,D[H+O+10]=r.z,D[H+O+11]=W.itemSize===4?r.w:1)}}f={count:d,texture:_,size:new Ue(I,R)},i.set(a,f),a.addEventListener("dispose",q)}if(s.isInstancedMesh===!0&&s.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",s.morphTexture,t);else{let m=0;for(let g=0;g<l.length;g++)m+=l[g];let y=a.morphTargetsRelative?1:1-m;c.getUniforms().setValue(n,"morphTargetBaseInfluence",y),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",f.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",f.size)}return{update:o}}function bO(n,e,t,i,r){let o=new WeakMap;function s(l){let u=r.render.frame,d=l.geometry,f=e.get(l,d);if(o.get(f)!==u&&(e.update(f),o.set(f,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),o.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),o.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;o.get(h)!==u&&(h.update(),o.set(h,u))}return f}function a(){o=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:s,dispose:a}}var SO={[Oy]:"LINEAR_TONE_MAPPING",[Fy]:"REINHARD_TONE_MAPPING",[ky]:"CINEON_TONE_MAPPING",[zl]:"ACES_FILMIC_TONE_MAPPING",[By]:"AGX_TONE_MAPPING",[Hy]:"NEUTRAL_TONE_MAPPING",[Uy]:"CUSTOM_TONE_MAPPING"};function wO(n,e,t,i,r){let o=new di(e,t,{type:n,depthBuffer:i,stencilBuffer:r}),s=new di(e,t,{type:pr,depthBuffer:!1,stencilBuffer:!1}),a=new _n;a.setAttribute("position",new ci([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new ci([0,2,0,0,2,0],2));let c=new Jf({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new Me(a,c),u=new Wa(-1,1,1,-1,0,1),d=null,f=null,h=!1,m,y=null,g=[],p=!1;this.setSize=function(E,w){o.setSize(E,w),s.setSize(E,w);for(let S=0;S<g.length;S++){let I=g[S];I.setSize&&I.setSize(E,w)}},this.setEffects=function(E){g=E,p=g.length>0&&g[0].isRenderPass===!0;let w=o.width,S=o.height;for(let I=0;I<g.length;I++){let R=g[I];R.setSize&&R.setSize(w,S)}},this.begin=function(E,w){if(h||E.toneMapping===qi&&g.length===0)return!1;if(y=w,w!==null){let S=w.width,I=w.height;(o.width!==S||o.height!==I)&&this.setSize(S,I)}return p===!1&&E.setRenderTarget(o),m=E.toneMapping,E.toneMapping=qi,!0},this.hasRenderPass=function(){return p},this.end=function(E,w){E.toneMapping=m,h=!0;let S=o,I=s;for(let R=0;R<g.length;R++){let D=g[R];if(D.enabled!==!1&&(D.render(E,I,S,w),D.needsSwap!==!1)){let _=S;S=I,I=_}}if(d!==E.outputColorSpace||f!==E.toneMapping){d=E.outputColorSpace,f=E.toneMapping,c.defines={},mt.getTransfer(d)===At&&(c.defines.SRGB_TRANSFER="");let R=SO[f];R&&(c.defines[R]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=S.texture,E.setRenderTarget(y),E.render(l,u),y=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){o.dispose(),s.dispose(),a.dispose(),c.dispose()}}var ew=new mr,l_=new bo(1,1),tw=new Cl,nw=new qf,iw=new Pl,FS=[],kS=[],US=new Float32Array(16),BS=new Float32Array(9),HS=new Float32Array(4);function Qa(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,o=FS[r];if(o===void 0&&(o=new Float32Array(r),FS[r]=o),e!==0){i.toArray(o,0);for(let s=1,a=0;s!==e;++s)a+=t,n[s].toArray(o,a)}return o}function sn(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function an(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function ap(n,e){let t=kS[e];t===void 0&&(t=new Int32Array(e),kS[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function TO(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function CO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(sn(t,e))return;n.uniform2fv(this.addr,e),an(t,e)}}function IO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(sn(t,e))return;n.uniform3fv(this.addr,e),an(t,e)}}function AO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(sn(t,e))return;n.uniform4fv(this.addr,e),an(t,e)}}function RO(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(sn(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),an(t,e)}else{if(sn(t,i))return;HS.set(i),n.uniformMatrix2fv(this.addr,!1,HS),an(t,i)}}function DO(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(sn(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),an(t,e)}else{if(sn(t,i))return;BS.set(i),n.uniformMatrix3fv(this.addr,!1,BS),an(t,i)}}function NO(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(sn(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),an(t,e)}else{if(sn(t,i))return;US.set(i),n.uniformMatrix4fv(this.addr,!1,US),an(t,i)}}function PO(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function LO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(sn(t,e))return;n.uniform2iv(this.addr,e),an(t,e)}}function OO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(sn(t,e))return;n.uniform3iv(this.addr,e),an(t,e)}}function FO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(sn(t,e))return;n.uniform4iv(this.addr,e),an(t,e)}}function kO(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function UO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(sn(t,e))return;n.uniform2uiv(this.addr,e),an(t,e)}}function BO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(sn(t,e))return;n.uniform3uiv(this.addr,e),an(t,e)}}function HO(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(sn(t,e))return;n.uniform4uiv(this.addr,e),an(t,e)}}function VO(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let o;this.type===n.SAMPLER_2D_SHADOW?(l_.compareFunction=t.isReversedDepthBuffer()?ep:Qh,o=l_):o=ew,t.setTexture2D(e||o,r)}function zO(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||nw,r)}function GO(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||iw,r)}function WO(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||tw,r)}function jO(n){switch(n){case 5126:return TO;case 35664:return CO;case 35665:return IO;case 35666:return AO;case 35674:return RO;case 35675:return DO;case 35676:return NO;case 5124:case 35670:return PO;case 35667:case 35671:return LO;case 35668:case 35672:return OO;case 35669:case 35673:return FO;case 5125:return kO;case 36294:return UO;case 36295:return BO;case 36296:return HO;case 35678:case 36198:case 36298:case 36306:case 35682:return VO;case 35679:case 36299:case 36307:return zO;case 35680:case 36300:case 36308:case 36293:return GO;case 36289:case 36303:case 36311:case 36292:return WO}}function $O(n,e){n.uniform1fv(this.addr,e)}function qO(n,e){let t=Qa(e,this.size,2);n.uniform2fv(this.addr,t)}function XO(n,e){let t=Qa(e,this.size,3);n.uniform3fv(this.addr,t)}function YO(n,e){let t=Qa(e,this.size,4);n.uniform4fv(this.addr,t)}function ZO(n,e){let t=Qa(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function KO(n,e){let t=Qa(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function JO(n,e){let t=Qa(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function QO(n,e){n.uniform1iv(this.addr,e)}function eF(n,e){n.uniform2iv(this.addr,e)}function tF(n,e){n.uniform3iv(this.addr,e)}function nF(n,e){n.uniform4iv(this.addr,e)}function iF(n,e){n.uniform1uiv(this.addr,e)}function rF(n,e){n.uniform2uiv(this.addr,e)}function oF(n,e){n.uniform3uiv(this.addr,e)}function sF(n,e){n.uniform4uiv(this.addr,e)}function aF(n,e,t){let i=this.cache,r=e.length,o=ap(t,r);sn(i,o)||(n.uniform1iv(this.addr,o),an(i,o));let s;this.type===n.SAMPLER_2D_SHADOW?s=l_:s=ew;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||s,o[a])}function cF(n,e,t){let i=this.cache,r=e.length,o=ap(t,r);sn(i,o)||(n.uniform1iv(this.addr,o),an(i,o));for(let s=0;s!==r;++s)t.setTexture3D(e[s]||nw,o[s])}function lF(n,e,t){let i=this.cache,r=e.length,o=ap(t,r);sn(i,o)||(n.uniform1iv(this.addr,o),an(i,o));for(let s=0;s!==r;++s)t.setTextureCube(e[s]||iw,o[s])}function uF(n,e,t){let i=this.cache,r=e.length,o=ap(t,r);sn(i,o)||(n.uniform1iv(this.addr,o),an(i,o));for(let s=0;s!==r;++s)t.setTexture2DArray(e[s]||tw,o[s])}function dF(n){switch(n){case 5126:return $O;case 35664:return qO;case 35665:return XO;case 35666:return YO;case 35674:return ZO;case 35675:return KO;case 35676:return JO;case 5124:case 35670:return QO;case 35667:case 35671:return eF;case 35668:case 35672:return tF;case 35669:case 35673:return nF;case 5125:return iF;case 36294:return rF;case 36295:return oF;case 36296:return sF;case 35678:case 36198:case 36298:case 36306:case 35682:return aF;case 35679:case 36299:case 36307:return cF;case 35680:case 36300:case 36308:case 36293:return lF;case 36289:case 36303:case 36311:case 36292:return uF}}var u_=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=jO(t.type)}},d_=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=dF(t.type)}},f_=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let o=0,s=r.length;o!==s;++o){let a=r[o];a.setValue(e,t[a.id],i)}}},a_=/(\w+)(\])?(\[|\.)?/g;function VS(n,e){n.seq.push(e),n.map[e.id]=e}function fF(n,e,t){let i=n.name,r=i.length;for(a_.lastIndex=0;;){let o=a_.exec(i),s=a_.lastIndex,a=o[1],c=o[2]==="]",l=o[3];if(c&&(a=a|0),l===void 0||l==="["&&s+2===r){VS(t,l===void 0?new u_(a,n,e):new d_(a,n,e));break}else{let d=t.map[a];d===void 0&&(d=new f_(a),VS(t,d)),t=d}}}var Ja=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let s=0;s<i;++s){let a=e.getActiveUniform(t,s),c=e.getUniformLocation(t,a.name);fF(a,c,this)}let r=[],o=[];for(let s of this.seq)s.type===e.SAMPLER_2D_SHADOW||s.type===e.SAMPLER_CUBE_SHADOW||s.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(s):o.push(s);r.length>0&&(this.seq=r.concat(o))}setValue(e,t,i,r){let o=this.map[t];o!==void 0&&o.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let o=0,s=t.length;o!==s;++o){let a=t[o],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,o=e.length;r!==o;++r){let s=e[r];s.id in t&&i.push(s)}return i}};function zS(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var hF=37297,pF=0;function mF(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),o=Math.min(e+6,t.length);for(let s=r;s<o;s++){let a=s+1;i.push(`${a===e?">":" "} ${a}: ${t[s]}`)}return i.join(`
`)}var GS=new et;function gF(n){mt._getMatrix(GS,mt.workingColorSpace,n);let e=`mat3( ${GS.elements.map(t=>t.toFixed(4))} )`;switch(mt.getTransfer(n)){case Sl:return[e,"LinearTransferOETF"];case At:return[e,"sRGBTransferOETF"];default:return Fe("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function WS(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),o=(n.getShaderInfoLog(e)||"").trim();if(i&&o==="")return"";let s=/ERROR: 0:(\d+)/.exec(o);if(s){let a=parseInt(s[1]);return t.toUpperCase()+`

`+o+`

`+mF(n.getShaderSource(e),a)}else return o}function vF(n,e){let t=gF(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var yF={[Oy]:"Linear",[Fy]:"Reinhard",[ky]:"Cineon",[zl]:"ACESFilmic",[By]:"AgX",[Hy]:"Neutral",[Uy]:"Custom"};function _F(n,e){let t=yF[e];return t===void 0?(Fe("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var np=new T;function xF(){mt.getLuminanceCoefficients(np);let n=np.x.toFixed(4),e=np.y.toFixed(4),t=np.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function EF(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(Kl).join(`
`)}function MF(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function bF(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let o=n.getActiveAttrib(e,r),s=o.name,a=1;o.type===n.FLOAT_MAT2&&(a=2),o.type===n.FLOAT_MAT3&&(a=3),o.type===n.FLOAT_MAT4&&(a=4),t[s]={type:o.type,location:n.getAttribLocation(e,s),locationSize:a}}return t}function Kl(n){return n!==""}function jS(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function $S(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var SF=/^[ \t]*#include +<([\w\d./]+)>/gm;function h_(n){return n.replace(SF,TF)}var wF=new Map;function TF(n,e){let t=nt[e];if(t===void 0){let i=wF.get(e);if(i!==void 0)t=nt[i],Fe('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return h_(t)}var CF=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function qS(n){return n.replace(CF,IF)}function IF(n,e,t,i){let r="";for(let o=parseInt(e);o<parseInt(t);o++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+o+" ]").replace(/UNROLLED_LOOP_INDEX/g,o);return r}function XS(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var AF={[Vl]:"SHADOWMAP_TYPE_PCF",[qa]:"SHADOWMAP_TYPE_VSM"};function RF(n){return AF[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var DF={[Ao]:"ENVMAP_TYPE_CUBE",[xs]:"ENVMAP_TYPE_CUBE",[Gl]:"ENVMAP_TYPE_CUBE_UV"};function NF(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":DF[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var PF={[xs]:"ENVMAP_MODE_REFRACTION"};function LF(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":PF[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var OF={[Ly]:"ENVMAP_BLENDING_MULTIPLY",[pS]:"ENVMAP_BLENDING_MIX",[mS]:"ENVMAP_BLENDING_ADD"};function FF(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":OF[n.combine]||"ENVMAP_BLENDING_NONE"}function kF(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function UF(n,e,t,i){let r=n.getContext(),o=t.defines,s=t.vertexShader,a=t.fragmentShader,c=RF(t),l=NF(t),u=LF(t),d=FF(t),f=kF(t),h=EF(t),m=MF(o),y=r.createProgram(),g,p,E=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(g=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m].filter(Kl).join(`
`),g.length>0&&(g+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m].filter(Kl).join(`
`),p.length>0&&(p+=`
`)):(g=[XS(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(Kl).join(`
`),p=[XS(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,m,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+d:"",f?"#define CUBEUV_TEXEL_WIDTH "+f.texelWidth:"",f?"#define CUBEUV_TEXEL_HEIGHT "+f.texelHeight:"",f?"#define CUBEUV_MAX_MIP "+f.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==qi?"#define TONE_MAPPING":"",t.toneMapping!==qi?nt.tonemapping_pars_fragment:"",t.toneMapping!==qi?_F("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",nt.colorspace_pars_fragment,vF("linearToOutputTexel",t.outputColorSpace),xF(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(Kl).join(`
`)),s=h_(s),s=jS(s,t),s=$S(s,t),a=h_(a),a=jS(a,t),a=$S(a,t),s=qS(s),a=qS(a),t.isRawShaderMaterial!==!0&&(E=`#version 300 es
`,g=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+g,p=["#define varying in",t.glslVersion===Yy?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===Yy?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let w=E+g+s,S=E+p+a,I=zS(r,r.VERTEX_SHADER,w),R=zS(r,r.FRAGMENT_SHADER,S);r.attachShader(y,I),r.attachShader(y,R),t.index0AttributeName!==void 0?r.bindAttribLocation(y,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(y,0,"position"),r.linkProgram(y);function D(C){if(n.debug.checkShaderErrors){let U=r.getProgramInfoLog(y)||"",B=r.getShaderInfoLog(I)||"",W=r.getShaderInfoLog(R)||"",H=U.trim(),V=B.trim(),O=W.trim(),ee=!0,Z=!0;if(r.getProgramParameter(y,r.LINK_STATUS)===!1)if(ee=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,y,I,R);else{let fe=WS(r,I,"vertex"),ye=WS(r,R,"fragment");ke("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(y,r.VALIDATE_STATUS)+`

Material Name: `+C.name+`
Material Type: `+C.type+`

Program Info Log: `+H+`
`+fe+`
`+ye)}else H!==""?Fe("WebGLProgram: Program Info Log:",H):(V===""||O==="")&&(Z=!1);Z&&(C.diagnostics={runnable:ee,programLog:H,vertexShader:{log:V,prefix:g},fragmentShader:{log:O,prefix:p}})}r.deleteShader(I),r.deleteShader(R),_=new Ja(r,y),M=bF(r,y)}let _;this.getUniforms=function(){return _===void 0&&D(this),_};let M;this.getAttributes=function(){return M===void 0&&D(this),M};let q=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return q===!1&&(q=r.getProgramParameter(y,hF)),q},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(y),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=pF++,this.cacheKey=e,this.usedTimes=1,this.program=y,this.vertexShader=I,this.fragmentShader=R,this}var BF=0,p_=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),o=this._getShaderStage(i),s=this._getShaderCacheForMaterial(e);return s.has(r)===!1&&(s.add(r),r.usedTimes++),s.has(o)===!1&&(s.add(o),o.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new m_(e),t.set(e,i)),i}},m_=class{constructor(e){this.id=BF++,this.code=e,this.usedTimes=0}};function HF(n,e,t,i,r,o){let s=new Ba,a=new p_,c=new Set,l=[],u=new Map,d=i.logarithmicDepthBuffer,f=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function m(_){return c.add(_),_===0?"uv":`uv${_}`}function y(_,M,q,C,U){let B=C.fog,W=U.geometry,H=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?C.environment:null,V=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap,O=e.get(_.envMap||H,V),ee=O&&O.mapping===Gl?O.image.height:null,Z=h[_.type];_.precision!==null&&(f=i.getMaxPrecision(_.precision),f!==_.precision&&Fe("WebGLProgram.getParameters:",_.precision,"not supported, using",f,"instead."));let fe=W.morphAttributes.position||W.morphAttributes.normal||W.morphAttributes.color,ye=fe!==void 0?fe.length:0,pe=0;W.morphAttributes.position!==void 0&&(pe=1),W.morphAttributes.normal!==void 0&&(pe=2),W.morphAttributes.color!==void 0&&(pe=3);let We,Dt,wt,X;if(Z){let vt=vr[Z];We=vt.vertexShader,Dt=vt.fragmentShader}else We=_.vertexShader,Dt=_.fragmentShader,a.update(_),wt=a.getVertexShaderID(_),X=a.getFragmentShaderID(_);let oe=n.getRenderTarget(),ae=n.state.buffers.depth.getReversed(),Xe=U.isInstancedMesh===!0,Le=U.isBatchedMesh===!0,Ge=!!_.map,$t=!!_.matcap,ht=!!O,gt=!!_.aoMap,Rt=!!_.lightMap,Be=!!_.bumpMap,Ht=!!_.normalMap,A=!!_.displacementMap,Gt=!!_.emissiveMap,ft=!!_.metalnessMap,ut=!!_.roughnessMap,be=_.anisotropy>0,b=_.clearcoat>0,v=_.dispersion>0,P=_.iridescence>0,$=_.sheen>0,Y=_.transmission>0,G=be&&!!_.anisotropyMap,ge=b&&!!_.clearcoatMap,te=b&&!!_.clearcoatNormalMap,Ae=b&&!!_.clearcoatRoughnessMap,Pe=P&&!!_.iridescenceMap,K=P&&!!_.iridescenceThicknessMap,re=$&&!!_.sheenColorMap,xe=$&&!!_.sheenRoughnessMap,Se=!!_.specularMap,he=!!_.specularColorMap,tt=!!_.specularIntensityMap,N=Y&&!!_.transmissionMap,ie=Y&&!!_.thicknessMap,Q=!!_.gradientMap,me=!!_.alphaMap,J=_.alphaTest>0,j=!!_.alphaHash,we=!!_.extensions,He=qi;_.toneMapped&&(oe===null||oe.isXRRenderTarget===!0)&&(He=n.toneMapping);let yt={shaderID:Z,shaderType:_.type,shaderName:_.name,vertexShader:We,fragmentShader:Dt,defines:_.defines,customVertexShaderID:wt,customFragmentShaderID:X,isRawShaderMaterial:_.isRawShaderMaterial===!0,glslVersion:_.glslVersion,precision:f,batching:Le,batchingColor:Le&&U._colorsTexture!==null,instancing:Xe,instancingColor:Xe&&U.instanceColor!==null,instancingMorph:Xe&&U.morphTexture!==null,outputColorSpace:oe===null?n.outputColorSpace:oe.isXRRenderTarget===!0?oe.texture.colorSpace:ms,alphaToCoverage:!!_.alphaToCoverage,map:Ge,matcap:$t,envMap:ht,envMapMode:ht&&O.mapping,envMapCubeUVHeight:ee,aoMap:gt,lightMap:Rt,bumpMap:Be,normalMap:Ht,displacementMap:A,emissiveMap:Gt,normalMapObjectSpace:Ht&&_.normalMapType===yS,normalMapTangentSpace:Ht&&_.normalMapType===Xy,metalnessMap:ft,roughnessMap:ut,anisotropy:be,anisotropyMap:G,clearcoat:b,clearcoatMap:ge,clearcoatNormalMap:te,clearcoatRoughnessMap:Ae,dispersion:v,iridescence:P,iridescenceMap:Pe,iridescenceThicknessMap:K,sheen:$,sheenColorMap:re,sheenRoughnessMap:xe,specularMap:Se,specularColorMap:he,specularIntensityMap:tt,transmission:Y,transmissionMap:N,thicknessMap:ie,gradientMap:Q,opaque:_.transparent===!1&&_.blending===hs&&_.alphaToCoverage===!1,alphaMap:me,alphaTest:J,alphaHash:j,combine:_.combine,mapUv:Ge&&m(_.map.channel),aoMapUv:gt&&m(_.aoMap.channel),lightMapUv:Rt&&m(_.lightMap.channel),bumpMapUv:Be&&m(_.bumpMap.channel),normalMapUv:Ht&&m(_.normalMap.channel),displacementMapUv:A&&m(_.displacementMap.channel),emissiveMapUv:Gt&&m(_.emissiveMap.channel),metalnessMapUv:ft&&m(_.metalnessMap.channel),roughnessMapUv:ut&&m(_.roughnessMap.channel),anisotropyMapUv:G&&m(_.anisotropyMap.channel),clearcoatMapUv:ge&&m(_.clearcoatMap.channel),clearcoatNormalMapUv:te&&m(_.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:Ae&&m(_.clearcoatRoughnessMap.channel),iridescenceMapUv:Pe&&m(_.iridescenceMap.channel),iridescenceThicknessMapUv:K&&m(_.iridescenceThicknessMap.channel),sheenColorMapUv:re&&m(_.sheenColorMap.channel),sheenRoughnessMapUv:xe&&m(_.sheenRoughnessMap.channel),specularMapUv:Se&&m(_.specularMap.channel),specularColorMapUv:he&&m(_.specularColorMap.channel),specularIntensityMapUv:tt&&m(_.specularIntensityMap.channel),transmissionMapUv:N&&m(_.transmissionMap.channel),thicknessMapUv:ie&&m(_.thicknessMap.channel),alphaMapUv:me&&m(_.alphaMap.channel),vertexTangents:!!W.attributes.tangent&&(Ht||be),vertexColors:_.vertexColors,vertexAlphas:_.vertexColors===!0&&!!W.attributes.color&&W.attributes.color.itemSize===4,pointsUvs:U.isPoints===!0&&!!W.attributes.uv&&(Ge||me),fog:!!B,useFog:_.fog===!0,fogExp2:!!B&&B.isFogExp2,flatShading:_.wireframe===!1&&(_.flatShading===!0||W.attributes.normal===void 0&&Ht===!1&&(_.isMeshLambertMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isMeshPhysicalMaterial)),sizeAttenuation:_.sizeAttenuation===!0,logarithmicDepthBuffer:d,reversedDepthBuffer:ae,skinning:U.isSkinnedMesh===!0,morphTargets:W.morphAttributes.position!==void 0,morphNormals:W.morphAttributes.normal!==void 0,morphColors:W.morphAttributes.color!==void 0,morphTargetsCount:ye,morphTextureStride:pe,numDirLights:M.directional.length,numPointLights:M.point.length,numSpotLights:M.spot.length,numSpotLightMaps:M.spotLightMap.length,numRectAreaLights:M.rectArea.length,numHemiLights:M.hemi.length,numDirLightShadows:M.directionalShadowMap.length,numPointLightShadows:M.pointShadowMap.length,numSpotLightShadows:M.spotShadowMap.length,numSpotLightShadowsWithMaps:M.numSpotLightShadowsWithMaps,numLightProbes:M.numLightProbes,numClippingPlanes:o.numPlanes,numClipIntersection:o.numIntersection,dithering:_.dithering,shadowMapEnabled:n.shadowMap.enabled&&q.length>0,shadowMapType:n.shadowMap.type,toneMapping:He,decodeVideoTexture:Ge&&_.map.isVideoTexture===!0&&mt.getTransfer(_.map.colorSpace)===At,decodeVideoTextureEmissive:Gt&&_.emissiveMap.isVideoTexture===!0&&mt.getTransfer(_.emissiveMap.colorSpace)===At,premultipliedAlpha:_.premultipliedAlpha,doubleSided:_.side===Jt,flipSided:_.side===zn,useDepthPacking:_.depthPacking>=0,depthPacking:_.depthPacking||0,index0AttributeName:_.index0AttributeName,extensionClipCullDistance:we&&_.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(we&&_.extensions.multiDraw===!0||Le)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:_.customProgramCacheKey()};return yt.vertexUv1s=c.has(1),yt.vertexUv2s=c.has(2),yt.vertexUv3s=c.has(3),c.clear(),yt}function g(_){let M=[];if(_.shaderID?M.push(_.shaderID):(M.push(_.customVertexShaderID),M.push(_.customFragmentShaderID)),_.defines!==void 0)for(let q in _.defines)M.push(q),M.push(_.defines[q]);return _.isRawShaderMaterial===!1&&(p(M,_),E(M,_),M.push(n.outputColorSpace)),M.push(_.customProgramCacheKey),M.join()}function p(_,M){_.push(M.precision),_.push(M.outputColorSpace),_.push(M.envMapMode),_.push(M.envMapCubeUVHeight),_.push(M.mapUv),_.push(M.alphaMapUv),_.push(M.lightMapUv),_.push(M.aoMapUv),_.push(M.bumpMapUv),_.push(M.normalMapUv),_.push(M.displacementMapUv),_.push(M.emissiveMapUv),_.push(M.metalnessMapUv),_.push(M.roughnessMapUv),_.push(M.anisotropyMapUv),_.push(M.clearcoatMapUv),_.push(M.clearcoatNormalMapUv),_.push(M.clearcoatRoughnessMapUv),_.push(M.iridescenceMapUv),_.push(M.iridescenceThicknessMapUv),_.push(M.sheenColorMapUv),_.push(M.sheenRoughnessMapUv),_.push(M.specularMapUv),_.push(M.specularColorMapUv),_.push(M.specularIntensityMapUv),_.push(M.transmissionMapUv),_.push(M.thicknessMapUv),_.push(M.combine),_.push(M.fogExp2),_.push(M.sizeAttenuation),_.push(M.morphTargetsCount),_.push(M.morphAttributeCount),_.push(M.numDirLights),_.push(M.numPointLights),_.push(M.numSpotLights),_.push(M.numSpotLightMaps),_.push(M.numHemiLights),_.push(M.numRectAreaLights),_.push(M.numDirLightShadows),_.push(M.numPointLightShadows),_.push(M.numSpotLightShadows),_.push(M.numSpotLightShadowsWithMaps),_.push(M.numLightProbes),_.push(M.shadowMapType),_.push(M.toneMapping),_.push(M.numClippingPlanes),_.push(M.numClipIntersection),_.push(M.depthPacking)}function E(_,M){s.disableAll(),M.instancing&&s.enable(0),M.instancingColor&&s.enable(1),M.instancingMorph&&s.enable(2),M.matcap&&s.enable(3),M.envMap&&s.enable(4),M.normalMapObjectSpace&&s.enable(5),M.normalMapTangentSpace&&s.enable(6),M.clearcoat&&s.enable(7),M.iridescence&&s.enable(8),M.alphaTest&&s.enable(9),M.vertexColors&&s.enable(10),M.vertexAlphas&&s.enable(11),M.vertexUv1s&&s.enable(12),M.vertexUv2s&&s.enable(13),M.vertexUv3s&&s.enable(14),M.vertexTangents&&s.enable(15),M.anisotropy&&s.enable(16),M.alphaHash&&s.enable(17),M.batching&&s.enable(18),M.dispersion&&s.enable(19),M.batchingColor&&s.enable(20),M.gradientMap&&s.enable(21),_.push(s.mask),s.disableAll(),M.fog&&s.enable(0),M.useFog&&s.enable(1),M.flatShading&&s.enable(2),M.logarithmicDepthBuffer&&s.enable(3),M.reversedDepthBuffer&&s.enable(4),M.skinning&&s.enable(5),M.morphTargets&&s.enable(6),M.morphNormals&&s.enable(7),M.morphColors&&s.enable(8),M.premultipliedAlpha&&s.enable(9),M.shadowMapEnabled&&s.enable(10),M.doubleSided&&s.enable(11),M.flipSided&&s.enable(12),M.useDepthPacking&&s.enable(13),M.dithering&&s.enable(14),M.transmission&&s.enable(15),M.sheen&&s.enable(16),M.opaque&&s.enable(17),M.pointsUvs&&s.enable(18),M.decodeVideoTexture&&s.enable(19),M.decodeVideoTextureEmissive&&s.enable(20),M.alphaToCoverage&&s.enable(21),_.push(s.mask)}function w(_){let M=h[_.type],q;if(M){let C=vr[M];q=RS.clone(C.uniforms)}else q=_.uniforms;return q}function S(_,M){let q=u.get(M);return q!==void 0?++q.usedTimes:(q=new UF(n,M,_,r),l.push(q),u.set(M,q)),q}function I(_){if(--_.usedTimes===0){let M=l.indexOf(_);l[M]=l[l.length-1],l.pop(),u.delete(_.cacheKey),_.destroy()}}function R(_){a.remove(_)}function D(){a.dispose()}return{getParameters:y,getProgramCacheKey:g,getUniforms:w,acquireProgram:S,releaseProgram:I,releaseShaderCache:R,programs:l,dispose:D}}function VF(){let n=new WeakMap;function e(s){return n.has(s)}function t(s){let a=n.get(s);return a===void 0&&(a={},n.set(s,a)),a}function i(s){n.delete(s)}function r(s,a,c){n.get(s)[a]=c}function o(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:o}}function zF(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function YS(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function ZS(){let n=[],e=0,t=[],i=[],r=[];function o(){e=0,t.length=0,i.length=0,r.length=0}function s(f){let h=0;return f.isInstancedMesh&&(h+=2),f.isSkinnedMesh&&(h+=1),h}function a(f,h,m,y,g,p){let E=n[e];return E===void 0?(E={id:f.id,object:f,geometry:h,material:m,materialVariant:s(f),groupOrder:y,renderOrder:f.renderOrder,z:g,group:p},n[e]=E):(E.id=f.id,E.object=f,E.geometry=h,E.material=m,E.materialVariant=s(f),E.groupOrder=y,E.renderOrder=f.renderOrder,E.z=g,E.group=p),e++,E}function c(f,h,m,y,g,p){let E=a(f,h,m,y,g,p);m.transmission>0?i.push(E):m.transparent===!0?r.push(E):t.push(E)}function l(f,h,m,y,g,p){let E=a(f,h,m,y,g,p);m.transmission>0?i.unshift(E):m.transparent===!0?r.unshift(E):t.unshift(E)}function u(f,h){t.length>1&&t.sort(f||zF),i.length>1&&i.sort(h||YS),r.length>1&&r.sort(h||YS)}function d(){for(let f=e,h=n.length;f<h;f++){let m=n[f];if(m.id===null)break;m.id=null,m.object=null,m.geometry=null,m.material=null,m.group=null}}return{opaque:t,transmissive:i,transparent:r,init:o,push:c,unshift:l,finish:d,sort:u}}function GF(){let n=new WeakMap;function e(i,r){let o=n.get(i),s;return o===void 0?(s=new ZS,n.set(i,[s])):r>=o.length?(s=new ZS,o.push(s)):s=o[r],s}function t(){n=new WeakMap}return{get:e,dispose:t}}function WF(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new T,color:new Qe};break;case"SpotLight":t={position:new T,direction:new T,color:new Qe,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new T,color:new Qe,distance:0,decay:0};break;case"HemisphereLight":t={direction:new T,skyColor:new Qe,groundColor:new Qe};break;case"RectAreaLight":t={color:new Qe,position:new T,halfWidth:new T,halfHeight:new T};break}return n[e.id]=t,t}}}function jF(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Ue};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Ue};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new Ue,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var $F=0;function qF(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function XF(n){let e=new WF,t=jF(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new T);let r=new T,o=new Bt,s=new Bt;function a(l){let u=0,d=0,f=0;for(let M=0;M<9;M++)i.probe[M].set(0,0,0);let h=0,m=0,y=0,g=0,p=0,E=0,w=0,S=0,I=0,R=0,D=0;l.sort(qF);for(let M=0,q=l.length;M<q;M++){let C=l[M],U=C.color,B=C.intensity,W=C.distance,H=null;if(C.shadow&&C.shadow.map&&(C.shadow.map.texture.format===Es?H=C.shadow.map.texture:H=C.shadow.map.depthTexture||C.shadow.map.texture),C.isAmbientLight)u+=U.r*B,d+=U.g*B,f+=U.b*B;else if(C.isLightProbe){for(let V=0;V<9;V++)i.probe[V].addScaledVector(C.sh.coefficients[V],B);D++}else if(C.isDirectionalLight){let V=e.get(C);if(V.color.copy(C.color).multiplyScalar(C.intensity),C.castShadow){let O=C.shadow,ee=t.get(C);ee.shadowIntensity=O.intensity,ee.shadowBias=O.bias,ee.shadowNormalBias=O.normalBias,ee.shadowRadius=O.radius,ee.shadowMapSize=O.mapSize,i.directionalShadow[h]=ee,i.directionalShadowMap[h]=H,i.directionalShadowMatrix[h]=C.shadow.matrix,E++}i.directional[h]=V,h++}else if(C.isSpotLight){let V=e.get(C);V.position.setFromMatrixPosition(C.matrixWorld),V.color.copy(U).multiplyScalar(B),V.distance=W,V.coneCos=Math.cos(C.angle),V.penumbraCos=Math.cos(C.angle*(1-C.penumbra)),V.decay=C.decay,i.spot[y]=V;let O=C.shadow;if(C.map&&(i.spotLightMap[I]=C.map,I++,O.updateMatrices(C),C.castShadow&&R++),i.spotLightMatrix[y]=O.matrix,C.castShadow){let ee=t.get(C);ee.shadowIntensity=O.intensity,ee.shadowBias=O.bias,ee.shadowNormalBias=O.normalBias,ee.shadowRadius=O.radius,ee.shadowMapSize=O.mapSize,i.spotShadow[y]=ee,i.spotShadowMap[y]=H,S++}y++}else if(C.isRectAreaLight){let V=e.get(C);V.color.copy(U).multiplyScalar(B),V.halfWidth.set(C.width*.5,0,0),V.halfHeight.set(0,C.height*.5,0),i.rectArea[g]=V,g++}else if(C.isPointLight){let V=e.get(C);if(V.color.copy(C.color).multiplyScalar(C.intensity),V.distance=C.distance,V.decay=C.decay,C.castShadow){let O=C.shadow,ee=t.get(C);ee.shadowIntensity=O.intensity,ee.shadowBias=O.bias,ee.shadowNormalBias=O.normalBias,ee.shadowRadius=O.radius,ee.shadowMapSize=O.mapSize,ee.shadowCameraNear=O.camera.near,ee.shadowCameraFar=O.camera.far,i.pointShadow[m]=ee,i.pointShadowMap[m]=H,i.pointShadowMatrix[m]=C.shadow.matrix,w++}i.point[m]=V,m++}else if(C.isHemisphereLight){let V=e.get(C);V.skyColor.copy(C.color).multiplyScalar(B),V.groundColor.copy(C.groundColor).multiplyScalar(B),i.hemi[p]=V,p++}}g>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=de.LTC_FLOAT_1,i.rectAreaLTC2=de.LTC_FLOAT_2):(i.rectAreaLTC1=de.LTC_HALF_1,i.rectAreaLTC2=de.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=d,i.ambient[2]=f;let _=i.hash;(_.directionalLength!==h||_.pointLength!==m||_.spotLength!==y||_.rectAreaLength!==g||_.hemiLength!==p||_.numDirectionalShadows!==E||_.numPointShadows!==w||_.numSpotShadows!==S||_.numSpotMaps!==I||_.numLightProbes!==D)&&(i.directional.length=h,i.spot.length=y,i.rectArea.length=g,i.point.length=m,i.hemi.length=p,i.directionalShadow.length=E,i.directionalShadowMap.length=E,i.pointShadow.length=w,i.pointShadowMap.length=w,i.spotShadow.length=S,i.spotShadowMap.length=S,i.directionalShadowMatrix.length=E,i.pointShadowMatrix.length=w,i.spotLightMatrix.length=S+I-R,i.spotLightMap.length=I,i.numSpotLightShadowsWithMaps=R,i.numLightProbes=D,_.directionalLength=h,_.pointLength=m,_.spotLength=y,_.rectAreaLength=g,_.hemiLength=p,_.numDirectionalShadows=E,_.numPointShadows=w,_.numSpotShadows=S,_.numSpotMaps=I,_.numLightProbes=D,i.version=$F++)}function c(l,u){let d=0,f=0,h=0,m=0,y=0,g=u.matrixWorldInverse;for(let p=0,E=l.length;p<E;p++){let w=l[p];if(w.isDirectionalLight){let S=i.directional[d];S.direction.setFromMatrixPosition(w.matrixWorld),r.setFromMatrixPosition(w.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(g),d++}else if(w.isSpotLight){let S=i.spot[h];S.position.setFromMatrixPosition(w.matrixWorld),S.position.applyMatrix4(g),S.direction.setFromMatrixPosition(w.matrixWorld),r.setFromMatrixPosition(w.target.matrixWorld),S.direction.sub(r),S.direction.transformDirection(g),h++}else if(w.isRectAreaLight){let S=i.rectArea[m];S.position.setFromMatrixPosition(w.matrixWorld),S.position.applyMatrix4(g),s.identity(),o.copy(w.matrixWorld),o.premultiply(g),s.extractRotation(o),S.halfWidth.set(w.width*.5,0,0),S.halfHeight.set(0,w.height*.5,0),S.halfWidth.applyMatrix4(s),S.halfHeight.applyMatrix4(s),m++}else if(w.isPointLight){let S=i.point[f];S.position.setFromMatrixPosition(w.matrixWorld),S.position.applyMatrix4(g),f++}else if(w.isHemisphereLight){let S=i.hemi[y];S.direction.setFromMatrixPosition(w.matrixWorld),S.direction.transformDirection(g),y++}}}return{setup:a,setupView:c,state:i}}function KS(n){let e=new XF(n),t=[],i=[];function r(u){l.camera=u,t.length=0,i.length=0}function o(u){t.push(u)}function s(u){i.push(u)}function a(){e.setup(t)}function c(u){e.setupView(t,u)}let l={lightsArray:t,shadowsArray:i,camera:null,lights:e,transmissionRenderTarget:{}};return{init:r,state:l,setupLights:a,setupLightsView:c,pushLight:o,pushShadow:s}}function YF(n){let e=new WeakMap;function t(r,o=0){let s=e.get(r),a;return s===void 0?(a=new KS(n),e.set(r,[a])):o>=s.length?(a=new KS(n),s.push(a)):a=s[o],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var ZF=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,KF=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,JF=[new T(1,0,0),new T(-1,0,0),new T(0,1,0),new T(0,-1,0),new T(0,0,1),new T(0,0,-1)],QF=[new T(0,-1,0),new T(0,-1,0),new T(0,0,1),new T(0,0,-1),new T(0,-1,0),new T(0,-1,0)],JS=new Bt,Zl=new T,c_=new T;function e2(n,e,t){let i=new Va,r=new Ue,o=new Ue,s=new jt,a=new Qf,c=new eh,l={},u=t.maxTextureSize,d={[Or]:zn,[zn]:Or,[Jt]:Jt},f=new fi({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new Ue},radius:{value:4}},vertexShader:ZF,fragmentShader:KF}),h=f.clone();h.defines.HORIZONTAL_PASS=1;let m=new _n;m.setAttribute("position",new vn(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let y=new Me(m,f),g=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Vl;let p=this.type;this.render=function(R,D,_){if(g.enabled===!1||g.autoUpdate===!1&&g.needsUpdate===!1||R.length===0)return;this.type===dh&&(Fe("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=Vl);let M=n.getRenderTarget(),q=n.getActiveCubeFace(),C=n.getActiveMipmapLevel(),U=n.state;U.setBlending(hr),U.buffers.depth.getReversed()===!0?U.buffers.color.setClear(0,0,0,0):U.buffers.color.setClear(1,1,1,1),U.buffers.depth.setTest(!0),U.setScissorTest(!1);let B=p!==this.type;B&&D.traverse(function(W){W.material&&(Array.isArray(W.material)?W.material.forEach(H=>H.needsUpdate=!0):W.material.needsUpdate=!0)});for(let W=0,H=R.length;W<H;W++){let V=R[W],O=V.shadow;if(O===void 0){Fe("WebGLShadowMap:",V,"has no shadow.");continue}if(O.autoUpdate===!1&&O.needsUpdate===!1)continue;r.copy(O.mapSize);let ee=O.getFrameExtents();r.multiply(ee),o.copy(O.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(o.x=Math.floor(u/ee.x),r.x=o.x*ee.x,O.mapSize.x=o.x),r.y>u&&(o.y=Math.floor(u/ee.y),r.y=o.y*ee.y,O.mapSize.y=o.y));let Z=n.state.buffers.depth.getReversed();if(O.camera._reversedDepth=Z,O.map===null||B===!0){if(O.map!==null&&(O.map.depthTexture!==null&&(O.map.depthTexture.dispose(),O.map.depthTexture=null),O.map.dispose()),this.type===qa){if(V.isPointLight){Fe("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}O.map=new di(r.x,r.y,{format:Es,type:pr,minFilter:Cn,magFilter:Cn,generateMipmaps:!1}),O.map.texture.name=V.name+".shadowMap",O.map.depthTexture=new bo(r.x,r.y,Yi),O.map.depthTexture.name=V.name+".shadowMapDepth",O.map.depthTexture.format=ur,O.map.depthTexture.compareFunction=null,O.map.depthTexture.minFilter=yn,O.map.depthTexture.magFilter=yn}else V.isPointLight?(O.map=new rp(r.x),O.map.depthTexture=new Kf(r.x,Xi)):(O.map=new di(r.x,r.y),O.map.depthTexture=new bo(r.x,r.y,Xi)),O.map.depthTexture.name=V.name+".shadowMap",O.map.depthTexture.format=ur,this.type===Vl?(O.map.depthTexture.compareFunction=Z?ep:Qh,O.map.depthTexture.minFilter=Cn,O.map.depthTexture.magFilter=Cn):(O.map.depthTexture.compareFunction=null,O.map.depthTexture.minFilter=yn,O.map.depthTexture.magFilter=yn);O.camera.updateProjectionMatrix()}let fe=O.map.isWebGLCubeRenderTarget?6:1;for(let ye=0;ye<fe;ye++){if(O.map.isWebGLCubeRenderTarget)n.setRenderTarget(O.map,ye),n.clear();else{ye===0&&(n.setRenderTarget(O.map),n.clear());let pe=O.getViewport(ye);s.set(o.x*pe.x,o.y*pe.y,o.x*pe.z,o.y*pe.w),U.viewport(s)}if(V.isPointLight){let pe=O.camera,We=O.matrix,Dt=V.distance||pe.far;Dt!==pe.far&&(pe.far=Dt,pe.updateProjectionMatrix()),Zl.setFromMatrixPosition(V.matrixWorld),pe.position.copy(Zl),c_.copy(pe.position),c_.add(JF[ye]),pe.up.copy(QF[ye]),pe.lookAt(c_),pe.updateMatrixWorld(),We.makeTranslation(-Zl.x,-Zl.y,-Zl.z),JS.multiplyMatrices(pe.projectionMatrix,pe.matrixWorldInverse),O._frustum.setFromProjectionMatrix(JS,pe.coordinateSystem,pe.reversedDepth)}else O.updateMatrices(V);i=O.getFrustum(),S(D,_,O.camera,V,this.type)}O.isPointLightShadow!==!0&&this.type===qa&&E(O,_),O.needsUpdate=!1}p=this.type,g.needsUpdate=!1,n.setRenderTarget(M,q,C)};function E(R,D){let _=e.update(y);f.defines.VSM_SAMPLES!==R.blurSamples&&(f.defines.VSM_SAMPLES=R.blurSamples,h.defines.VSM_SAMPLES=R.blurSamples,f.needsUpdate=!0,h.needsUpdate=!0),R.mapPass===null&&(R.mapPass=new di(r.x,r.y,{format:Es,type:pr})),f.uniforms.shadow_pass.value=R.map.depthTexture,f.uniforms.resolution.value=R.mapSize,f.uniforms.radius.value=R.radius,n.setRenderTarget(R.mapPass),n.clear(),n.renderBufferDirect(D,null,_,f,y,null),h.uniforms.shadow_pass.value=R.mapPass.texture,h.uniforms.resolution.value=R.mapSize,h.uniforms.radius.value=R.radius,n.setRenderTarget(R.map),n.clear(),n.renderBufferDirect(D,null,_,h,y,null)}function w(R,D,_,M){let q=null,C=_.isPointLight===!0?R.customDistanceMaterial:R.customDepthMaterial;if(C!==void 0)q=C;else if(q=_.isPointLight===!0?c:a,n.localClippingEnabled&&D.clipShadows===!0&&Array.isArray(D.clippingPlanes)&&D.clippingPlanes.length!==0||D.displacementMap&&D.displacementScale!==0||D.alphaMap&&D.alphaTest>0||D.map&&D.alphaTest>0||D.alphaToCoverage===!0){let U=q.uuid,B=D.uuid,W=l[U];W===void 0&&(W={},l[U]=W);let H=W[B];H===void 0&&(H=q.clone(),W[B]=H,D.addEventListener("dispose",I)),q=H}if(q.visible=D.visible,q.wireframe=D.wireframe,M===qa?q.side=D.shadowSide!==null?D.shadowSide:D.side:q.side=D.shadowSide!==null?D.shadowSide:d[D.side],q.alphaMap=D.alphaMap,q.alphaTest=D.alphaToCoverage===!0?.5:D.alphaTest,q.map=D.map,q.clipShadows=D.clipShadows,q.clippingPlanes=D.clippingPlanes,q.clipIntersection=D.clipIntersection,q.displacementMap=D.displacementMap,q.displacementScale=D.displacementScale,q.displacementBias=D.displacementBias,q.wireframeLinewidth=D.wireframeLinewidth,q.linewidth=D.linewidth,_.isPointLight===!0&&q.isMeshDistanceMaterial===!0){let U=n.properties.get(q);U.light=_}return q}function S(R,D,_,M,q){if(R.visible===!1)return;if(R.layers.test(D.layers)&&(R.isMesh||R.isLine||R.isPoints)&&(R.castShadow||R.receiveShadow&&q===qa)&&(!R.frustumCulled||i.intersectsObject(R))){R.modelViewMatrix.multiplyMatrices(_.matrixWorldInverse,R.matrixWorld);let B=e.update(R),W=R.material;if(Array.isArray(W)){let H=B.groups;for(let V=0,O=H.length;V<O;V++){let ee=H[V],Z=W[ee.materialIndex];if(Z&&Z.visible){let fe=w(R,Z,M,q);R.onBeforeShadow(n,R,D,_,B,fe,ee),n.renderBufferDirect(_,null,B,fe,R,ee),R.onAfterShadow(n,R,D,_,B,fe,ee)}}}else if(W.visible){let H=w(R,W,M,q);R.onBeforeShadow(n,R,D,_,B,H,null),n.renderBufferDirect(_,null,B,H,R,null),R.onAfterShadow(n,R,D,_,B,H,null)}}let U=R.children;for(let B=0,W=U.length;B<W;B++)S(U[B],D,_,M,q)}function I(R){R.target.removeEventListener("dispose",I);for(let _ in l){let M=l[_],q=R.target.uuid;q in M&&(M[q].dispose(),delete M[q])}}}function t2(n,e){function t(){let N=!1,ie=new jt,Q=null,me=new jt(0,0,0,0);return{setMask:function(J){Q!==J&&!N&&(n.colorMask(J,J,J,J),Q=J)},setLocked:function(J){N=J},setClear:function(J,j,we,He,yt){yt===!0&&(J*=He,j*=He,we*=He),ie.set(J,j,we,He),me.equals(ie)===!1&&(n.clearColor(J,j,we,He),me.copy(ie))},reset:function(){N=!1,Q=null,me.set(-1,0,0,0)}}}function i(){let N=!1,ie=!1,Q=null,me=null,J=null;return{setReversed:function(j){if(ie!==j){let we=e.get("EXT_clip_control");j?we.clipControlEXT(we.LOWER_LEFT_EXT,we.ZERO_TO_ONE_EXT):we.clipControlEXT(we.LOWER_LEFT_EXT,we.NEGATIVE_ONE_TO_ONE_EXT),ie=j;let He=J;J=null,this.setClear(He)}},getReversed:function(){return ie},setTest:function(j){j?oe(n.DEPTH_TEST):ae(n.DEPTH_TEST)},setMask:function(j){Q!==j&&!N&&(n.depthMask(j),Q=j)},setFunc:function(j){if(ie&&(j=IS[j]),me!==j){switch(j){case Ff:n.depthFunc(n.NEVER);break;case kf:n.depthFunc(n.ALWAYS);break;case Uf:n.depthFunc(n.LESS);break;case ps:n.depthFunc(n.LEQUAL);break;case Bf:n.depthFunc(n.EQUAL);break;case Hf:n.depthFunc(n.GEQUAL);break;case Vf:n.depthFunc(n.GREATER);break;case zf:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}me=j}},setLocked:function(j){N=j},setClear:function(j){J!==j&&(J=j,ie&&(j=1-j),n.clearDepth(j))},reset:function(){N=!1,Q=null,me=null,J=null,ie=!1}}}function r(){let N=!1,ie=null,Q=null,me=null,J=null,j=null,we=null,He=null,yt=null;return{setTest:function(vt){N||(vt?oe(n.STENCIL_TEST):ae(n.STENCIL_TEST))},setMask:function(vt){ie!==vt&&!N&&(n.stencilMask(vt),ie=vt)},setFunc:function(vt,Wn,Ci){(Q!==vt||me!==Wn||J!==Ci)&&(n.stencilFunc(vt,Wn,Ci),Q=vt,me=Wn,J=Ci)},setOp:function(vt,Wn,Ci){(j!==vt||we!==Wn||He!==Ci)&&(n.stencilOp(vt,Wn,Ci),j=vt,we=Wn,He=Ci)},setLocked:function(vt){N=vt},setClear:function(vt){yt!==vt&&(n.clearStencil(vt),yt=vt)},reset:function(){N=!1,ie=null,Q=null,me=null,J=null,j=null,we=null,He=null,yt=null}}}let o=new t,s=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},d={},f=new WeakMap,h=[],m=null,y=!1,g=null,p=null,E=null,w=null,S=null,I=null,R=null,D=new Qe(0,0,0),_=0,M=!1,q=null,C=null,U=null,B=null,W=null,H=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),V=!1,O=0,ee=n.getParameter(n.VERSION);ee.indexOf("WebGL")!==-1?(O=parseFloat(/^WebGL (\d)/.exec(ee)[1]),V=O>=1):ee.indexOf("OpenGL ES")!==-1&&(O=parseFloat(/^OpenGL ES (\d)/.exec(ee)[1]),V=O>=2);let Z=null,fe={},ye=n.getParameter(n.SCISSOR_BOX),pe=n.getParameter(n.VIEWPORT),We=new jt().fromArray(ye),Dt=new jt().fromArray(pe);function wt(N,ie,Q,me){let J=new Uint8Array(4),j=n.createTexture();n.bindTexture(N,j),n.texParameteri(N,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(N,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let we=0;we<Q;we++)N===n.TEXTURE_3D||N===n.TEXTURE_2D_ARRAY?n.texImage3D(ie,0,n.RGBA,1,1,me,0,n.RGBA,n.UNSIGNED_BYTE,J):n.texImage2D(ie+we,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,J);return j}let X={};X[n.TEXTURE_2D]=wt(n.TEXTURE_2D,n.TEXTURE_2D,1),X[n.TEXTURE_CUBE_MAP]=wt(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),X[n.TEXTURE_2D_ARRAY]=wt(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),X[n.TEXTURE_3D]=wt(n.TEXTURE_3D,n.TEXTURE_3D,1,1),o.setClear(0,0,0,1),s.setClear(1),a.setClear(0),oe(n.DEPTH_TEST),s.setFunc(ps),Be(!1),Ht(Ry),oe(n.CULL_FACE),gt(hr);function oe(N){u[N]!==!0&&(n.enable(N),u[N]=!0)}function ae(N){u[N]!==!1&&(n.disable(N),u[N]=!1)}function Xe(N,ie){return d[N]!==ie?(n.bindFramebuffer(N,ie),d[N]=ie,N===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=ie),N===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=ie),!0):!1}function Le(N,ie){let Q=h,me=!1;if(N){Q=f.get(ie),Q===void 0&&(Q=[],f.set(ie,Q));let J=N.textures;if(Q.length!==J.length||Q[0]!==n.COLOR_ATTACHMENT0){for(let j=0,we=J.length;j<we;j++)Q[j]=n.COLOR_ATTACHMENT0+j;Q.length=J.length,me=!0}}else Q[0]!==n.BACK&&(Q[0]=n.BACK,me=!0);me&&n.drawBuffers(Q)}function Ge(N){return m!==N?(n.useProgram(N),m=N,!0):!1}let $t={[_o]:n.FUNC_ADD,[Kb]:n.FUNC_SUBTRACT,[Jb]:n.FUNC_REVERSE_SUBTRACT};$t[Qb]=n.MIN,$t[eS]=n.MAX;let ht={[tS]:n.ZERO,[nS]:n.ONE,[iS]:n.SRC_COLOR,[Lf]:n.SRC_ALPHA,[lS]:n.SRC_ALPHA_SATURATE,[aS]:n.DST_COLOR,[oS]:n.DST_ALPHA,[rS]:n.ONE_MINUS_SRC_COLOR,[Of]:n.ONE_MINUS_SRC_ALPHA,[cS]:n.ONE_MINUS_DST_COLOR,[sS]:n.ONE_MINUS_DST_ALPHA,[uS]:n.CONSTANT_COLOR,[dS]:n.ONE_MINUS_CONSTANT_COLOR,[fS]:n.CONSTANT_ALPHA,[hS]:n.ONE_MINUS_CONSTANT_ALPHA};function gt(N,ie,Q,me,J,j,we,He,yt,vt){if(N===hr){y===!0&&(ae(n.BLEND),y=!1);return}if(y===!1&&(oe(n.BLEND),y=!0),N!==Zb){if(N!==g||vt!==M){if((p!==_o||S!==_o)&&(n.blendEquation(n.FUNC_ADD),p=_o,S=_o),vt)switch(N){case hs:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Dy:n.blendFunc(n.ONE,n.ONE);break;case Ny:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Py:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:ke("WebGLState: Invalid blending: ",N);break}else switch(N){case hs:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Dy:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case Ny:ke("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Py:ke("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:ke("WebGLState: Invalid blending: ",N);break}E=null,w=null,I=null,R=null,D.set(0,0,0),_=0,g=N,M=vt}return}J=J||ie,j=j||Q,we=we||me,(ie!==p||J!==S)&&(n.blendEquationSeparate($t[ie],$t[J]),p=ie,S=J),(Q!==E||me!==w||j!==I||we!==R)&&(n.blendFuncSeparate(ht[Q],ht[me],ht[j],ht[we]),E=Q,w=me,I=j,R=we),(He.equals(D)===!1||yt!==_)&&(n.blendColor(He.r,He.g,He.b,yt),D.copy(He),_=yt),g=N,M=!1}function Rt(N,ie){N.side===Jt?ae(n.CULL_FACE):oe(n.CULL_FACE);let Q=N.side===zn;ie&&(Q=!Q),Be(Q),N.blending===hs&&N.transparent===!1?gt(hr):gt(N.blending,N.blendEquation,N.blendSrc,N.blendDst,N.blendEquationAlpha,N.blendSrcAlpha,N.blendDstAlpha,N.blendColor,N.blendAlpha,N.premultipliedAlpha),s.setFunc(N.depthFunc),s.setTest(N.depthTest),s.setMask(N.depthWrite),o.setMask(N.colorWrite);let me=N.stencilWrite;a.setTest(me),me&&(a.setMask(N.stencilWriteMask),a.setFunc(N.stencilFunc,N.stencilRef,N.stencilFuncMask),a.setOp(N.stencilFail,N.stencilZFail,N.stencilZPass)),Gt(N.polygonOffset,N.polygonOffsetFactor,N.polygonOffsetUnits),N.alphaToCoverage===!0?oe(n.SAMPLE_ALPHA_TO_COVERAGE):ae(n.SAMPLE_ALPHA_TO_COVERAGE)}function Be(N){q!==N&&(N?n.frontFace(n.CW):n.frontFace(n.CCW),q=N)}function Ht(N){N!==Xb?(oe(n.CULL_FACE),N!==C&&(N===Ry?n.cullFace(n.BACK):N===Yb?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):ae(n.CULL_FACE),C=N}function A(N){N!==U&&(V&&n.lineWidth(N),U=N)}function Gt(N,ie,Q){N?(oe(n.POLYGON_OFFSET_FILL),(B!==ie||W!==Q)&&(B=ie,W=Q,s.getReversed()&&(ie=-ie),n.polygonOffset(ie,Q))):ae(n.POLYGON_OFFSET_FILL)}function ft(N){N?oe(n.SCISSOR_TEST):ae(n.SCISSOR_TEST)}function ut(N){N===void 0&&(N=n.TEXTURE0+H-1),Z!==N&&(n.activeTexture(N),Z=N)}function be(N,ie,Q){Q===void 0&&(Z===null?Q=n.TEXTURE0+H-1:Q=Z);let me=fe[Q];me===void 0&&(me={type:void 0,texture:void 0},fe[Q]=me),(me.type!==N||me.texture!==ie)&&(Z!==Q&&(n.activeTexture(Q),Z=Q),n.bindTexture(N,ie||X[N]),me.type=N,me.texture=ie)}function b(){let N=fe[Z];N!==void 0&&N.type!==void 0&&(n.bindTexture(N.type,null),N.type=void 0,N.texture=void 0)}function v(){try{n.compressedTexImage2D(...arguments)}catch(N){ke("WebGLState:",N)}}function P(){try{n.compressedTexImage3D(...arguments)}catch(N){ke("WebGLState:",N)}}function $(){try{n.texSubImage2D(...arguments)}catch(N){ke("WebGLState:",N)}}function Y(){try{n.texSubImage3D(...arguments)}catch(N){ke("WebGLState:",N)}}function G(){try{n.compressedTexSubImage2D(...arguments)}catch(N){ke("WebGLState:",N)}}function ge(){try{n.compressedTexSubImage3D(...arguments)}catch(N){ke("WebGLState:",N)}}function te(){try{n.texStorage2D(...arguments)}catch(N){ke("WebGLState:",N)}}function Ae(){try{n.texStorage3D(...arguments)}catch(N){ke("WebGLState:",N)}}function Pe(){try{n.texImage2D(...arguments)}catch(N){ke("WebGLState:",N)}}function K(){try{n.texImage3D(...arguments)}catch(N){ke("WebGLState:",N)}}function re(N){We.equals(N)===!1&&(n.scissor(N.x,N.y,N.z,N.w),We.copy(N))}function xe(N){Dt.equals(N)===!1&&(n.viewport(N.x,N.y,N.z,N.w),Dt.copy(N))}function Se(N,ie){let Q=l.get(ie);Q===void 0&&(Q=new WeakMap,l.set(ie,Q));let me=Q.get(N);me===void 0&&(me=n.getUniformBlockIndex(ie,N.name),Q.set(N,me))}function he(N,ie){let me=l.get(ie).get(N);c.get(ie)!==me&&(n.uniformBlockBinding(ie,me,N.__bindingPointIndex),c.set(ie,me))}function tt(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),s.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),u={},Z=null,fe={},d={},f=new WeakMap,h=[],m=null,y=!1,g=null,p=null,E=null,w=null,S=null,I=null,R=null,D=new Qe(0,0,0),_=0,M=!1,q=null,C=null,U=null,B=null,W=null,We.set(0,0,n.canvas.width,n.canvas.height),Dt.set(0,0,n.canvas.width,n.canvas.height),o.reset(),s.reset(),a.reset()}return{buffers:{color:o,depth:s,stencil:a},enable:oe,disable:ae,bindFramebuffer:Xe,drawBuffers:Le,useProgram:Ge,setBlending:gt,setMaterial:Rt,setFlipSided:Be,setCullFace:Ht,setLineWidth:A,setPolygonOffset:Gt,setScissorTest:ft,activeTexture:ut,bindTexture:be,unbindTexture:b,compressedTexImage2D:v,compressedTexImage3D:P,texImage2D:Pe,texImage3D:K,updateUBOMapping:Se,uniformBlockBinding:he,texStorage2D:te,texStorage3D:Ae,texSubImage2D:$,texSubImage3D:Y,compressedTexSubImage2D:G,compressedTexSubImage3D:ge,scissor:re,viewport:xe,reset:tt}}function n2(n,e,t,i,r,o,s){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new Ue,u=new WeakMap,d,f=new WeakMap,h=!1;try{h=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function m(b,v){return h?new OffscreenCanvas(b,v):wl("canvas")}function y(b,v,P){let $=1,Y=be(b);if((Y.width>P||Y.height>P)&&($=P/Math.max(Y.width,Y.height)),$<1)if(typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&b instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&b instanceof ImageBitmap||typeof VideoFrame<"u"&&b instanceof VideoFrame){let G=Math.floor($*Y.width),ge=Math.floor($*Y.height);d===void 0&&(d=m(G,ge));let te=v?m(G,ge):d;return te.width=G,te.height=ge,te.getContext("2d").drawImage(b,0,0,G,ge),Fe("WebGLRenderer: Texture has been resized from ("+Y.width+"x"+Y.height+") to ("+G+"x"+ge+")."),te}else return"data"in b&&Fe("WebGLRenderer: Image in DataTexture is too big ("+Y.width+"x"+Y.height+")."),b;return b}function g(b){return b.generateMipmaps}function p(b){n.generateMipmap(b)}function E(b){return b.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:b.isWebGL3DRenderTarget?n.TEXTURE_3D:b.isWebGLArrayRenderTarget||b.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function w(b,v,P,$,Y=!1){if(b!==null){if(n[b]!==void 0)return n[b];Fe("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+b+"'")}let G=v;if(v===n.RED&&(P===n.FLOAT&&(G=n.R32F),P===n.HALF_FLOAT&&(G=n.R16F),P===n.UNSIGNED_BYTE&&(G=n.R8)),v===n.RED_INTEGER&&(P===n.UNSIGNED_BYTE&&(G=n.R8UI),P===n.UNSIGNED_SHORT&&(G=n.R16UI),P===n.UNSIGNED_INT&&(G=n.R32UI),P===n.BYTE&&(G=n.R8I),P===n.SHORT&&(G=n.R16I),P===n.INT&&(G=n.R32I)),v===n.RG&&(P===n.FLOAT&&(G=n.RG32F),P===n.HALF_FLOAT&&(G=n.RG16F),P===n.UNSIGNED_BYTE&&(G=n.RG8)),v===n.RG_INTEGER&&(P===n.UNSIGNED_BYTE&&(G=n.RG8UI),P===n.UNSIGNED_SHORT&&(G=n.RG16UI),P===n.UNSIGNED_INT&&(G=n.RG32UI),P===n.BYTE&&(G=n.RG8I),P===n.SHORT&&(G=n.RG16I),P===n.INT&&(G=n.RG32I)),v===n.RGB_INTEGER&&(P===n.UNSIGNED_BYTE&&(G=n.RGB8UI),P===n.UNSIGNED_SHORT&&(G=n.RGB16UI),P===n.UNSIGNED_INT&&(G=n.RGB32UI),P===n.BYTE&&(G=n.RGB8I),P===n.SHORT&&(G=n.RGB16I),P===n.INT&&(G=n.RGB32I)),v===n.RGBA_INTEGER&&(P===n.UNSIGNED_BYTE&&(G=n.RGBA8UI),P===n.UNSIGNED_SHORT&&(G=n.RGBA16UI),P===n.UNSIGNED_INT&&(G=n.RGBA32UI),P===n.BYTE&&(G=n.RGBA8I),P===n.SHORT&&(G=n.RGBA16I),P===n.INT&&(G=n.RGBA32I)),v===n.RGB&&(P===n.UNSIGNED_INT_5_9_9_9_REV&&(G=n.RGB9_E5),P===n.UNSIGNED_INT_10F_11F_11F_REV&&(G=n.R11F_G11F_B10F)),v===n.RGBA){let ge=Y?Sl:mt.getTransfer($);P===n.FLOAT&&(G=n.RGBA32F),P===n.HALF_FLOAT&&(G=n.RGBA16F),P===n.UNSIGNED_BYTE&&(G=ge===At?n.SRGB8_ALPHA8:n.RGBA8),P===n.UNSIGNED_SHORT_4_4_4_4&&(G=n.RGBA4),P===n.UNSIGNED_SHORT_5_5_5_1&&(G=n.RGB5_A1)}return(G===n.R16F||G===n.R32F||G===n.RG16F||G===n.RG32F||G===n.RGBA16F||G===n.RGBA32F)&&e.get("EXT_color_buffer_float"),G}function S(b,v){let P;return b?v===null||v===Xi||v===Ya?P=n.DEPTH24_STENCIL8:v===Yi?P=n.DEPTH32F_STENCIL8:v===Xa&&(P=n.DEPTH24_STENCIL8,Fe("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):v===null||v===Xi||v===Ya?P=n.DEPTH_COMPONENT24:v===Yi?P=n.DEPTH_COMPONENT32F:v===Xa&&(P=n.DEPTH_COMPONENT16),P}function I(b,v){return g(b)===!0||b.isFramebufferTexture&&b.minFilter!==yn&&b.minFilter!==Cn?Math.log2(Math.max(v.width,v.height))+1:b.mipmaps!==void 0&&b.mipmaps.length>0?b.mipmaps.length:b.isCompressedTexture&&Array.isArray(b.image)?v.mipmaps.length:1}function R(b){let v=b.target;v.removeEventListener("dispose",R),_(v),v.isVideoTexture&&u.delete(v)}function D(b){let v=b.target;v.removeEventListener("dispose",D),q(v)}function _(b){let v=i.get(b);if(v.__webglInit===void 0)return;let P=b.source,$=f.get(P);if($){let Y=$[v.__cacheKey];Y.usedTimes--,Y.usedTimes===0&&M(b),Object.keys($).length===0&&f.delete(P)}i.remove(b)}function M(b){let v=i.get(b);n.deleteTexture(v.__webglTexture);let P=b.source,$=f.get(P);delete $[v.__cacheKey],s.memory.textures--}function q(b){let v=i.get(b);if(b.depthTexture&&(b.depthTexture.dispose(),i.remove(b.depthTexture)),b.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(v.__webglFramebuffer[$]))for(let Y=0;Y<v.__webglFramebuffer[$].length;Y++)n.deleteFramebuffer(v.__webglFramebuffer[$][Y]);else n.deleteFramebuffer(v.__webglFramebuffer[$]);v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer[$])}else{if(Array.isArray(v.__webglFramebuffer))for(let $=0;$<v.__webglFramebuffer.length;$++)n.deleteFramebuffer(v.__webglFramebuffer[$]);else n.deleteFramebuffer(v.__webglFramebuffer);if(v.__webglDepthbuffer&&n.deleteRenderbuffer(v.__webglDepthbuffer),v.__webglMultisampledFramebuffer&&n.deleteFramebuffer(v.__webglMultisampledFramebuffer),v.__webglColorRenderbuffer)for(let $=0;$<v.__webglColorRenderbuffer.length;$++)v.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(v.__webglColorRenderbuffer[$]);v.__webglDepthRenderbuffer&&n.deleteRenderbuffer(v.__webglDepthRenderbuffer)}let P=b.textures;for(let $=0,Y=P.length;$<Y;$++){let G=i.get(P[$]);G.__webglTexture&&(n.deleteTexture(G.__webglTexture),s.memory.textures--),i.remove(P[$])}i.remove(b)}let C=0;function U(){C=0}function B(){let b=C;return b>=r.maxTextures&&Fe("WebGLTextures: Trying to use "+b+" texture units while this GPU supports only "+r.maxTextures),C+=1,b}function W(b){let v=[];return v.push(b.wrapS),v.push(b.wrapT),v.push(b.wrapR||0),v.push(b.magFilter),v.push(b.minFilter),v.push(b.anisotropy),v.push(b.internalFormat),v.push(b.format),v.push(b.type),v.push(b.generateMipmaps),v.push(b.premultiplyAlpha),v.push(b.flipY),v.push(b.unpackAlignment),v.push(b.colorSpace),v.join()}function H(b,v){let P=i.get(b);if(b.isVideoTexture&&ft(b),b.isRenderTargetTexture===!1&&b.isExternalTexture!==!0&&b.version>0&&P.__version!==b.version){let $=b.image;if($===null)Fe("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)Fe("WebGLRenderer: Texture marked for update but image is incomplete");else{X(P,b,v);return}}else b.isExternalTexture&&(P.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,P.__webglTexture,n.TEXTURE0+v)}function V(b,v){let P=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&P.__version!==b.version){X(P,b,v);return}else b.isExternalTexture&&(P.__webglTexture=b.sourceTexture?b.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,P.__webglTexture,n.TEXTURE0+v)}function O(b,v){let P=i.get(b);if(b.isRenderTargetTexture===!1&&b.version>0&&P.__version!==b.version){X(P,b,v);return}t.bindTexture(n.TEXTURE_3D,P.__webglTexture,n.TEXTURE0+v)}function ee(b,v){let P=i.get(b);if(b.isCubeDepthTexture!==!0&&b.version>0&&P.__version!==b.version){oe(P,b,v);return}t.bindTexture(n.TEXTURE_CUBE_MAP,P.__webglTexture,n.TEXTURE0+v)}let Z={[li]:n.REPEAT,[lr]:n.CLAMP_TO_EDGE,[Gf]:n.MIRRORED_REPEAT},fe={[yn]:n.NEAREST,[gS]:n.NEAREST_MIPMAP_NEAREST,[Wl]:n.NEAREST_MIPMAP_LINEAR,[Cn]:n.LINEAR,[ph]:n.LINEAR_MIPMAP_NEAREST,[Ro]:n.LINEAR_MIPMAP_LINEAR},ye={[_S]:n.NEVER,[SS]:n.ALWAYS,[xS]:n.LESS,[Qh]:n.LEQUAL,[ES]:n.EQUAL,[ep]:n.GEQUAL,[MS]:n.GREATER,[bS]:n.NOTEQUAL};function pe(b,v){if(v.type===Yi&&e.has("OES_texture_float_linear")===!1&&(v.magFilter===Cn||v.magFilter===ph||v.magFilter===Wl||v.magFilter===Ro||v.minFilter===Cn||v.minFilter===ph||v.minFilter===Wl||v.minFilter===Ro)&&Fe("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(b,n.TEXTURE_WRAP_S,Z[v.wrapS]),n.texParameteri(b,n.TEXTURE_WRAP_T,Z[v.wrapT]),(b===n.TEXTURE_3D||b===n.TEXTURE_2D_ARRAY)&&n.texParameteri(b,n.TEXTURE_WRAP_R,Z[v.wrapR]),n.texParameteri(b,n.TEXTURE_MAG_FILTER,fe[v.magFilter]),n.texParameteri(b,n.TEXTURE_MIN_FILTER,fe[v.minFilter]),v.compareFunction&&(n.texParameteri(b,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(b,n.TEXTURE_COMPARE_FUNC,ye[v.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(v.magFilter===yn||v.minFilter!==Wl&&v.minFilter!==Ro||v.type===Yi&&e.has("OES_texture_float_linear")===!1)return;if(v.anisotropy>1||i.get(v).__currentAnisotropy){let P=e.get("EXT_texture_filter_anisotropic");n.texParameterf(b,P.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(v.anisotropy,r.getMaxAnisotropy())),i.get(v).__currentAnisotropy=v.anisotropy}}}function We(b,v){let P=!1;b.__webglInit===void 0&&(b.__webglInit=!0,v.addEventListener("dispose",R));let $=v.source,Y=f.get($);Y===void 0&&(Y={},f.set($,Y));let G=W(v);if(G!==b.__cacheKey){Y[G]===void 0&&(Y[G]={texture:n.createTexture(),usedTimes:0},s.memory.textures++,P=!0),Y[G].usedTimes++;let ge=Y[b.__cacheKey];ge!==void 0&&(Y[b.__cacheKey].usedTimes--,ge.usedTimes===0&&M(v)),b.__cacheKey=G,b.__webglTexture=Y[G].texture}return P}function Dt(b,v,P){return Math.floor(Math.floor(b/P)/v)}function wt(b,v,P,$){let G=b.updateRanges;if(G.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,v.width,v.height,P,$,v.data);else{G.sort((K,re)=>K.start-re.start);let ge=0;for(let K=1;K<G.length;K++){let re=G[ge],xe=G[K],Se=re.start+re.count,he=Dt(xe.start,v.width,4),tt=Dt(re.start,v.width,4);xe.start<=Se+1&&he===tt&&Dt(xe.start+xe.count-1,v.width,4)===he?re.count=Math.max(re.count,xe.start+xe.count-re.start):(++ge,G[ge]=xe)}G.length=ge+1;let te=n.getParameter(n.UNPACK_ROW_LENGTH),Ae=n.getParameter(n.UNPACK_SKIP_PIXELS),Pe=n.getParameter(n.UNPACK_SKIP_ROWS);n.pixelStorei(n.UNPACK_ROW_LENGTH,v.width);for(let K=0,re=G.length;K<re;K++){let xe=G[K],Se=Math.floor(xe.start/4),he=Math.ceil(xe.count/4),tt=Se%v.width,N=Math.floor(Se/v.width),ie=he,Q=1;n.pixelStorei(n.UNPACK_SKIP_PIXELS,tt),n.pixelStorei(n.UNPACK_SKIP_ROWS,N),t.texSubImage2D(n.TEXTURE_2D,0,tt,N,ie,Q,P,$,v.data)}b.clearUpdateRanges(),n.pixelStorei(n.UNPACK_ROW_LENGTH,te),n.pixelStorei(n.UNPACK_SKIP_PIXELS,Ae),n.pixelStorei(n.UNPACK_SKIP_ROWS,Pe)}}function X(b,v,P){let $=n.TEXTURE_2D;(v.isDataArrayTexture||v.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),v.isData3DTexture&&($=n.TEXTURE_3D);let Y=We(b,v),G=v.source;t.bindTexture($,b.__webglTexture,n.TEXTURE0+P);let ge=i.get(G);if(G.version!==ge.__version||Y===!0){t.activeTexture(n.TEXTURE0+P);let te=mt.getPrimaries(mt.workingColorSpace),Ae=v.colorSpace===kr?null:mt.getPrimaries(v.colorSpace),Pe=v.colorSpace===kr||te===Ae?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Pe);let K=y(v.image,!1,r.maxTextureSize);K=ut(v,K);let re=o.convert(v.format,v.colorSpace),xe=o.convert(v.type),Se=w(v.internalFormat,re,xe,v.colorSpace,v.isVideoTexture);pe($,v);let he,tt=v.mipmaps,N=v.isVideoTexture!==!0,ie=ge.__version===void 0||Y===!0,Q=G.dataReady,me=I(v,K);if(v.isDepthTexture)Se=S(v.format===Do,v.type),ie&&(N?t.texStorage2D(n.TEXTURE_2D,1,Se,K.width,K.height):t.texImage2D(n.TEXTURE_2D,0,Se,K.width,K.height,0,re,xe,null));else if(v.isDataTexture)if(tt.length>0){N&&ie&&t.texStorage2D(n.TEXTURE_2D,me,Se,tt[0].width,tt[0].height);for(let J=0,j=tt.length;J<j;J++)he=tt[J],N?Q&&t.texSubImage2D(n.TEXTURE_2D,J,0,0,he.width,he.height,re,xe,he.data):t.texImage2D(n.TEXTURE_2D,J,Se,he.width,he.height,0,re,xe,he.data);v.generateMipmaps=!1}else N?(ie&&t.texStorage2D(n.TEXTURE_2D,me,Se,K.width,K.height),Q&&wt(v,K,re,xe)):t.texImage2D(n.TEXTURE_2D,0,Se,K.width,K.height,0,re,xe,K.data);else if(v.isCompressedTexture)if(v.isCompressedArrayTexture){N&&ie&&t.texStorage3D(n.TEXTURE_2D_ARRAY,me,Se,tt[0].width,tt[0].height,K.depth);for(let J=0,j=tt.length;J<j;J++)if(he=tt[J],v.format!==Ti)if(re!==null)if(N){if(Q)if(v.layerUpdates.size>0){let we=n_(he.width,he.height,v.format,v.type);for(let He of v.layerUpdates){let yt=he.data.subarray(He*we/he.data.BYTES_PER_ELEMENT,(He+1)*we/he.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,J,0,0,He,he.width,he.height,1,re,yt)}v.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,J,0,0,0,he.width,he.height,K.depth,re,he.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,J,Se,he.width,he.height,K.depth,0,he.data,0,0);else Fe("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else N?Q&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,J,0,0,0,he.width,he.height,K.depth,re,xe,he.data):t.texImage3D(n.TEXTURE_2D_ARRAY,J,Se,he.width,he.height,K.depth,0,re,xe,he.data)}else{N&&ie&&t.texStorage2D(n.TEXTURE_2D,me,Se,tt[0].width,tt[0].height);for(let J=0,j=tt.length;J<j;J++)he=tt[J],v.format!==Ti?re!==null?N?Q&&t.compressedTexSubImage2D(n.TEXTURE_2D,J,0,0,he.width,he.height,re,he.data):t.compressedTexImage2D(n.TEXTURE_2D,J,Se,he.width,he.height,0,he.data):Fe("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):N?Q&&t.texSubImage2D(n.TEXTURE_2D,J,0,0,he.width,he.height,re,xe,he.data):t.texImage2D(n.TEXTURE_2D,J,Se,he.width,he.height,0,re,xe,he.data)}else if(v.isDataArrayTexture)if(N){if(ie&&t.texStorage3D(n.TEXTURE_2D_ARRAY,me,Se,K.width,K.height,K.depth),Q)if(v.layerUpdates.size>0){let J=n_(K.width,K.height,v.format,v.type);for(let j of v.layerUpdates){let we=K.data.subarray(j*J/K.data.BYTES_PER_ELEMENT,(j+1)*J/K.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,j,K.width,K.height,1,re,xe,we)}v.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,K.width,K.height,K.depth,re,xe,K.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,Se,K.width,K.height,K.depth,0,re,xe,K.data);else if(v.isData3DTexture)N?(ie&&t.texStorage3D(n.TEXTURE_3D,me,Se,K.width,K.height,K.depth),Q&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,K.width,K.height,K.depth,re,xe,K.data)):t.texImage3D(n.TEXTURE_3D,0,Se,K.width,K.height,K.depth,0,re,xe,K.data);else if(v.isFramebufferTexture){if(ie)if(N)t.texStorage2D(n.TEXTURE_2D,me,Se,K.width,K.height);else{let J=K.width,j=K.height;for(let we=0;we<me;we++)t.texImage2D(n.TEXTURE_2D,we,Se,J,j,0,re,xe,null),J>>=1,j>>=1}}else if(tt.length>0){if(N&&ie){let J=be(tt[0]);t.texStorage2D(n.TEXTURE_2D,me,Se,J.width,J.height)}for(let J=0,j=tt.length;J<j;J++)he=tt[J],N?Q&&t.texSubImage2D(n.TEXTURE_2D,J,0,0,re,xe,he):t.texImage2D(n.TEXTURE_2D,J,Se,re,xe,he);v.generateMipmaps=!1}else if(N){if(ie){let J=be(K);t.texStorage2D(n.TEXTURE_2D,me,Se,J.width,J.height)}Q&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,re,xe,K)}else t.texImage2D(n.TEXTURE_2D,0,Se,re,xe,K);g(v)&&p($),ge.__version=G.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function oe(b,v,P){if(v.image.length!==6)return;let $=We(b,v),Y=v.source;t.bindTexture(n.TEXTURE_CUBE_MAP,b.__webglTexture,n.TEXTURE0+P);let G=i.get(Y);if(Y.version!==G.__version||$===!0){t.activeTexture(n.TEXTURE0+P);let ge=mt.getPrimaries(mt.workingColorSpace),te=v.colorSpace===kr?null:mt.getPrimaries(v.colorSpace),Ae=v.colorSpace===kr||ge===te?n.NONE:n.BROWSER_DEFAULT_WEBGL;n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,v.flipY),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,v.premultiplyAlpha),n.pixelStorei(n.UNPACK_ALIGNMENT,v.unpackAlignment),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,Ae);let Pe=v.isCompressedTexture||v.image[0].isCompressedTexture,K=v.image[0]&&v.image[0].isDataTexture,re=[];for(let j=0;j<6;j++)!Pe&&!K?re[j]=y(v.image[j],!0,r.maxCubemapSize):re[j]=K?v.image[j].image:v.image[j],re[j]=ut(v,re[j]);let xe=re[0],Se=o.convert(v.format,v.colorSpace),he=o.convert(v.type),tt=w(v.internalFormat,Se,he,v.colorSpace),N=v.isVideoTexture!==!0,ie=G.__version===void 0||$===!0,Q=Y.dataReady,me=I(v,xe);pe(n.TEXTURE_CUBE_MAP,v);let J;if(Pe){N&&ie&&t.texStorage2D(n.TEXTURE_CUBE_MAP,me,tt,xe.width,xe.height);for(let j=0;j<6;j++){J=re[j].mipmaps;for(let we=0;we<J.length;we++){let He=J[we];v.format!==Ti?Se!==null?N?Q&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we,0,0,He.width,He.height,Se,He.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we,tt,He.width,He.height,0,He.data):Fe("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):N?Q&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we,0,0,He.width,He.height,Se,he,He.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we,tt,He.width,He.height,0,Se,he,He.data)}}}else{if(J=v.mipmaps,N&&ie){J.length>0&&me++;let j=be(re[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,me,tt,j.width,j.height)}for(let j=0;j<6;j++)if(K){N?Q&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,0,0,re[j].width,re[j].height,Se,he,re[j].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,tt,re[j].width,re[j].height,0,Se,he,re[j].data);for(let we=0;we<J.length;we++){let yt=J[we].image[j].image;N?Q&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we+1,0,0,yt.width,yt.height,Se,he,yt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we+1,tt,yt.width,yt.height,0,Se,he,yt.data)}}else{N?Q&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,0,0,Se,he,re[j]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,0,tt,Se,he,re[j]);for(let we=0;we<J.length;we++){let He=J[we];N?Q&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we+1,0,0,Se,he,He.image[j]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+j,we+1,tt,Se,he,He.image[j])}}}g(v)&&p(n.TEXTURE_CUBE_MAP),G.__version=Y.version,v.onUpdate&&v.onUpdate(v)}b.__version=v.version}function ae(b,v,P,$,Y,G){let ge=o.convert(P.format,P.colorSpace),te=o.convert(P.type),Ae=w(P.internalFormat,ge,te,P.colorSpace),Pe=i.get(v),K=i.get(P);if(K.__renderTarget=v,!Pe.__hasExternalTextures){let re=Math.max(1,v.width>>G),xe=Math.max(1,v.height>>G);Y===n.TEXTURE_3D||Y===n.TEXTURE_2D_ARRAY?t.texImage3D(Y,G,Ae,re,xe,v.depth,0,ge,te,null):t.texImage2D(Y,G,Ae,re,xe,0,ge,te,null)}t.bindFramebuffer(n.FRAMEBUFFER,b),Gt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,Y,K.__webglTexture,0,A(v)):(Y===n.TEXTURE_2D||Y>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&Y<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,Y,K.__webglTexture,G),t.bindFramebuffer(n.FRAMEBUFFER,null)}function Xe(b,v,P){if(n.bindRenderbuffer(n.RENDERBUFFER,b),v.depthBuffer){let $=v.depthTexture,Y=$&&$.isDepthTexture?$.type:null,G=S(v.stencilBuffer,Y),ge=v.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;Gt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,A(v),G,v.width,v.height):P?n.renderbufferStorageMultisample(n.RENDERBUFFER,A(v),G,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,G,v.width,v.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,ge,n.RENDERBUFFER,b)}else{let $=v.textures;for(let Y=0;Y<$.length;Y++){let G=$[Y],ge=o.convert(G.format,G.colorSpace),te=o.convert(G.type),Ae=w(G.internalFormat,ge,te,G.colorSpace);Gt(v)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,A(v),Ae,v.width,v.height):P?n.renderbufferStorageMultisample(n.RENDERBUFFER,A(v),Ae,v.width,v.height):n.renderbufferStorage(n.RENDERBUFFER,Ae,v.width,v.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Le(b,v,P){let $=v.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,b),!(v.depthTexture&&v.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let Y=i.get(v.depthTexture);if(Y.__renderTarget=v,(!Y.__webglTexture||v.depthTexture.image.width!==v.width||v.depthTexture.image.height!==v.height)&&(v.depthTexture.image.width=v.width,v.depthTexture.image.height=v.height,v.depthTexture.needsUpdate=!0),$){if(Y.__webglInit===void 0&&(Y.__webglInit=!0,v.depthTexture.addEventListener("dispose",R)),Y.__webglTexture===void 0){Y.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,Y.__webglTexture),pe(n.TEXTURE_CUBE_MAP,v.depthTexture);let Pe=o.convert(v.depthTexture.format),K=o.convert(v.depthTexture.type),re;v.depthTexture.format===ur?re=n.DEPTH_COMPONENT24:v.depthTexture.format===Do&&(re=n.DEPTH24_STENCIL8);for(let xe=0;xe<6;xe++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+xe,0,re,v.width,v.height,0,Pe,K,null)}}else H(v.depthTexture,0);let G=Y.__webglTexture,ge=A(v),te=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+P:n.TEXTURE_2D,Ae=v.depthTexture.format===Do?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(v.depthTexture.format===ur)Gt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Ae,te,G,0,ge):n.framebufferTexture2D(n.FRAMEBUFFER,Ae,te,G,0);else if(v.depthTexture.format===Do)Gt(v)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,Ae,te,G,0,ge):n.framebufferTexture2D(n.FRAMEBUFFER,Ae,te,G,0);else throw new Error("Unknown depthTexture format")}function Ge(b){let v=i.get(b),P=b.isWebGLCubeRenderTarget===!0;if(v.__boundDepthTexture!==b.depthTexture){let $=b.depthTexture;if(v.__depthDisposeCallback&&v.__depthDisposeCallback(),$){let Y=()=>{delete v.__boundDepthTexture,delete v.__depthDisposeCallback,$.removeEventListener("dispose",Y)};$.addEventListener("dispose",Y),v.__depthDisposeCallback=Y}v.__boundDepthTexture=$}if(b.depthTexture&&!v.__autoAllocateDepthBuffer)if(P)for(let $=0;$<6;$++)Le(v.__webglFramebuffer[$],b,$);else{let $=b.texture.mipmaps;$&&$.length>0?Le(v.__webglFramebuffer[0],b,0):Le(v.__webglFramebuffer,b,0)}else if(P){v.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[$]),v.__webglDepthbuffer[$]===void 0)v.__webglDepthbuffer[$]=n.createRenderbuffer(),Xe(v.__webglDepthbuffer[$],b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,G=v.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,G),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,G)}}else{let $=b.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,v.__webglFramebuffer),v.__webglDepthbuffer===void 0)v.__webglDepthbuffer=n.createRenderbuffer(),Xe(v.__webglDepthbuffer,b,!1);else{let Y=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,G=v.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,G),n.framebufferRenderbuffer(n.FRAMEBUFFER,Y,n.RENDERBUFFER,G)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function $t(b,v,P){let $=i.get(b);v!==void 0&&ae($.__webglFramebuffer,b,b.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),P!==void 0&&Ge(b)}function ht(b){let v=b.texture,P=i.get(b),$=i.get(v);b.addEventListener("dispose",D);let Y=b.textures,G=b.isWebGLCubeRenderTarget===!0,ge=Y.length>1;if(ge||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=v.version,s.memory.textures++),G){P.__webglFramebuffer=[];for(let te=0;te<6;te++)if(v.mipmaps&&v.mipmaps.length>0){P.__webglFramebuffer[te]=[];for(let Ae=0;Ae<v.mipmaps.length;Ae++)P.__webglFramebuffer[te][Ae]=n.createFramebuffer()}else P.__webglFramebuffer[te]=n.createFramebuffer()}else{if(v.mipmaps&&v.mipmaps.length>0){P.__webglFramebuffer=[];for(let te=0;te<v.mipmaps.length;te++)P.__webglFramebuffer[te]=n.createFramebuffer()}else P.__webglFramebuffer=n.createFramebuffer();if(ge)for(let te=0,Ae=Y.length;te<Ae;te++){let Pe=i.get(Y[te]);Pe.__webglTexture===void 0&&(Pe.__webglTexture=n.createTexture(),s.memory.textures++)}if(b.samples>0&&Gt(b)===!1){P.__webglMultisampledFramebuffer=n.createFramebuffer(),P.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,P.__webglMultisampledFramebuffer);for(let te=0;te<Y.length;te++){let Ae=Y[te];P.__webglColorRenderbuffer[te]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,P.__webglColorRenderbuffer[te]);let Pe=o.convert(Ae.format,Ae.colorSpace),K=o.convert(Ae.type),re=w(Ae.internalFormat,Pe,K,Ae.colorSpace,b.isXRRenderTarget===!0),xe=A(b);n.renderbufferStorageMultisample(n.RENDERBUFFER,xe,re,b.width,b.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+te,n.RENDERBUFFER,P.__webglColorRenderbuffer[te])}n.bindRenderbuffer(n.RENDERBUFFER,null),b.depthBuffer&&(P.__webglDepthRenderbuffer=n.createRenderbuffer(),Xe(P.__webglDepthRenderbuffer,b,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(G){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),pe(n.TEXTURE_CUBE_MAP,v);for(let te=0;te<6;te++)if(v.mipmaps&&v.mipmaps.length>0)for(let Ae=0;Ae<v.mipmaps.length;Ae++)ae(P.__webglFramebuffer[te][Ae],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+te,Ae);else ae(P.__webglFramebuffer[te],b,v,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0);g(v)&&p(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(ge){for(let te=0,Ae=Y.length;te<Ae;te++){let Pe=Y[te],K=i.get(Pe),re=n.TEXTURE_2D;(b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(re=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(re,K.__webglTexture),pe(re,Pe),ae(P.__webglFramebuffer,b,Pe,n.COLOR_ATTACHMENT0+te,re,0),g(Pe)&&p(re)}t.unbindTexture()}else{let te=n.TEXTURE_2D;if((b.isWebGL3DRenderTarget||b.isWebGLArrayRenderTarget)&&(te=b.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(te,$.__webglTexture),pe(te,v),v.mipmaps&&v.mipmaps.length>0)for(let Ae=0;Ae<v.mipmaps.length;Ae++)ae(P.__webglFramebuffer[Ae],b,v,n.COLOR_ATTACHMENT0,te,Ae);else ae(P.__webglFramebuffer,b,v,n.COLOR_ATTACHMENT0,te,0);g(v)&&p(te),t.unbindTexture()}b.depthBuffer&&Ge(b)}function gt(b){let v=b.textures;for(let P=0,$=v.length;P<$;P++){let Y=v[P];if(g(Y)){let G=E(b),ge=i.get(Y).__webglTexture;t.bindTexture(G,ge),p(G),t.unbindTexture()}}}let Rt=[],Be=[];function Ht(b){if(b.samples>0){if(Gt(b)===!1){let v=b.textures,P=b.width,$=b.height,Y=n.COLOR_BUFFER_BIT,G=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ge=i.get(b),te=v.length>1;if(te)for(let Pe=0;Pe<v.length;Pe++)t.bindFramebuffer(n.FRAMEBUFFER,ge.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,ge.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,ge.__webglMultisampledFramebuffer);let Ae=b.texture.mipmaps;Ae&&Ae.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ge.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ge.__webglFramebuffer);for(let Pe=0;Pe<v.length;Pe++){if(b.resolveDepthBuffer&&(b.depthBuffer&&(Y|=n.DEPTH_BUFFER_BIT),b.stencilBuffer&&b.resolveStencilBuffer&&(Y|=n.STENCIL_BUFFER_BIT)),te){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,ge.__webglColorRenderbuffer[Pe]);let K=i.get(v[Pe]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,K,0)}n.blitFramebuffer(0,0,P,$,0,0,P,$,Y,n.NEAREST),c===!0&&(Rt.length=0,Be.length=0,Rt.push(n.COLOR_ATTACHMENT0+Pe),b.depthBuffer&&b.resolveDepthBuffer===!1&&(Rt.push(G),Be.push(G),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,Be)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,Rt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),te)for(let Pe=0;Pe<v.length;Pe++){t.bindFramebuffer(n.FRAMEBUFFER,ge.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.RENDERBUFFER,ge.__webglColorRenderbuffer[Pe]);let K=i.get(v[Pe]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,ge.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+Pe,n.TEXTURE_2D,K,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,ge.__webglMultisampledFramebuffer)}else if(b.depthBuffer&&b.resolveDepthBuffer===!1&&c){let v=b.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[v])}}}function A(b){return Math.min(r.maxSamples,b.samples)}function Gt(b){let v=i.get(b);return b.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&v.__useRenderToTexture!==!1}function ft(b){let v=s.render.frame;u.get(b)!==v&&(u.set(b,v),b.update())}function ut(b,v){let P=b.colorSpace,$=b.format,Y=b.type;return b.isCompressedTexture===!0||b.isVideoTexture===!0||P!==ms&&P!==kr&&(mt.getTransfer(P)===At?($!==Ti||Y!==Yn)&&Fe("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):ke("WebGLTextures: Unsupported texture color space:",P)),v}function be(b){return typeof HTMLImageElement<"u"&&b instanceof HTMLImageElement?(l.width=b.naturalWidth||b.width,l.height=b.naturalHeight||b.height):typeof VideoFrame<"u"&&b instanceof VideoFrame?(l.width=b.displayWidth,l.height=b.displayHeight):(l.width=b.width,l.height=b.height),l}this.allocateTextureUnit=B,this.resetTextureUnits=U,this.setTexture2D=H,this.setTexture2DArray=V,this.setTexture3D=O,this.setTextureCube=ee,this.rebindTextures=$t,this.setupRenderTarget=ht,this.updateRenderTargetMipmap=gt,this.updateMultisampleRenderTarget=Ht,this.setupDepthRenderbuffer=Ge,this.setupFrameBufferTexture=ae,this.useMultisampledRTT=Gt,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function i2(n,e){function t(i,r=kr){let o,s=mt.getTransfer(r);if(i===Yn)return n.UNSIGNED_BYTE;if(i===gh)return n.UNSIGNED_SHORT_4_4_4_4;if(i===vh)return n.UNSIGNED_SHORT_5_5_5_1;if(i===Gy)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===Wy)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Vy)return n.BYTE;if(i===zy)return n.SHORT;if(i===Xa)return n.UNSIGNED_SHORT;if(i===mh)return n.INT;if(i===Xi)return n.UNSIGNED_INT;if(i===Yi)return n.FLOAT;if(i===pr)return n.HALF_FLOAT;if(i===jy)return n.ALPHA;if(i===$y)return n.RGB;if(i===Ti)return n.RGBA;if(i===ur)return n.DEPTH_COMPONENT;if(i===Do)return n.DEPTH_STENCIL;if(i===qy)return n.RED;if(i===yh)return n.RED_INTEGER;if(i===Es)return n.RG;if(i===_h)return n.RG_INTEGER;if(i===xh)return n.RGBA_INTEGER;if(i===jl||i===$l||i===ql||i===Xl)if(s===At)if(o=e.get("WEBGL_compressed_texture_s3tc_srgb"),o!==null){if(i===jl)return o.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===$l)return o.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===ql)return o.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===Xl)return o.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(o=e.get("WEBGL_compressed_texture_s3tc"),o!==null){if(i===jl)return o.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===$l)return o.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===ql)return o.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===Xl)return o.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Eh||i===Mh||i===bh||i===Sh)if(o=e.get("WEBGL_compressed_texture_pvrtc"),o!==null){if(i===Eh)return o.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===Mh)return o.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===bh)return o.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Sh)return o.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===wh||i===Th||i===Ch||i===Ih||i===Ah||i===Rh||i===Dh)if(o=e.get("WEBGL_compressed_texture_etc"),o!==null){if(i===wh||i===Th)return s===At?o.COMPRESSED_SRGB8_ETC2:o.COMPRESSED_RGB8_ETC2;if(i===Ch)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:o.COMPRESSED_RGBA8_ETC2_EAC;if(i===Ih)return o.COMPRESSED_R11_EAC;if(i===Ah)return o.COMPRESSED_SIGNED_R11_EAC;if(i===Rh)return o.COMPRESSED_RG11_EAC;if(i===Dh)return o.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===Nh||i===Ph||i===Lh||i===Oh||i===Fh||i===kh||i===Uh||i===Bh||i===Hh||i===Vh||i===zh||i===Gh||i===Wh||i===jh)if(o=e.get("WEBGL_compressed_texture_astc"),o!==null){if(i===Nh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:o.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Ph)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:o.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===Lh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:o.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===Oh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:o.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===Fh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:o.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===kh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:o.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Uh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:o.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Bh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:o.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Hh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:o.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Vh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:o.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===zh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:o.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===Gh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:o.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===Wh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:o.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===jh)return s===At?o.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:o.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===$h||i===qh||i===Xh)if(o=e.get("EXT_texture_compression_bptc"),o!==null){if(i===$h)return s===At?o.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:o.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===qh)return o.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===Xh)return o.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===Yh||i===Zh||i===Kh||i===Jh)if(o=e.get("EXT_texture_compression_rgtc"),o!==null){if(i===Yh)return o.COMPRESSED_RED_RGTC1_EXT;if(i===Zh)return o.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===Kh)return o.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===Jh)return o.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Ya?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var r2=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,o2=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,g_=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Ll(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new fi({vertexShader:r2,fragmentShader:o2,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Me(new Wt(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},v_=class extends dr{constructor(e,t){super();let i=this,r=null,o=1,s=null,a="local-floor",c=1,l=null,u=null,d=null,f=null,h=null,m=null,y=typeof XRWebGLBinding<"u",g=new g_,p={},E=t.getContextAttributes(),w=null,S=null,I=[],R=[],D=new Ue,_=null,M=new On;M.viewport=new jt;let q=new On;q.viewport=new jt;let C=[M,q],U=new uh,B=null,W=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(X){let oe=I[X];return oe===void 0&&(oe=new Ha,I[X]=oe),oe.getTargetRaySpace()},this.getControllerGrip=function(X){let oe=I[X];return oe===void 0&&(oe=new Ha,I[X]=oe),oe.getGripSpace()},this.getHand=function(X){let oe=I[X];return oe===void 0&&(oe=new Ha,I[X]=oe),oe.getHandSpace()};function H(X){let oe=R.indexOf(X.inputSource);if(oe===-1)return;let ae=I[oe];ae!==void 0&&(ae.update(X.inputSource,X.frame,l||s),ae.dispatchEvent({type:X.type,data:X.inputSource}))}function V(){r.removeEventListener("select",H),r.removeEventListener("selectstart",H),r.removeEventListener("selectend",H),r.removeEventListener("squeeze",H),r.removeEventListener("squeezestart",H),r.removeEventListener("squeezeend",H),r.removeEventListener("end",V),r.removeEventListener("inputsourceschange",O);for(let X=0;X<I.length;X++){let oe=R[X];oe!==null&&(R[X]=null,I[X].disconnect(oe))}B=null,W=null,g.reset();for(let X in p)delete p[X];e.setRenderTarget(w),h=null,f=null,d=null,r=null,S=null,wt.stop(),i.isPresenting=!1,e.setPixelRatio(_),e.setSize(D.width,D.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(X){o=X,i.isPresenting===!0&&Fe("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(X){a=X,i.isPresenting===!0&&Fe("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||s},this.setReferenceSpace=function(X){l=X},this.getBaseLayer=function(){return f!==null?f:h},this.getBinding=function(){return d===null&&y&&(d=new XRWebGLBinding(r,t)),d},this.getFrame=function(){return m},this.getSession=function(){return r},this.setSession=async function(X){if(r=X,r!==null){if(w=e.getRenderTarget(),r.addEventListener("select",H),r.addEventListener("selectstart",H),r.addEventListener("selectend",H),r.addEventListener("squeeze",H),r.addEventListener("squeezestart",H),r.addEventListener("squeezeend",H),r.addEventListener("end",V),r.addEventListener("inputsourceschange",O),E.xrCompatible!==!0&&await t.makeXRCompatible(),_=e.getPixelRatio(),e.getSize(D),y&&"createProjectionLayer"in XRWebGLBinding.prototype){let ae=null,Xe=null,Le=null;E.depth&&(Le=E.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ae=E.stencil?Do:ur,Xe=E.stencil?Ya:Xi);let Ge={colorFormat:t.RGBA8,depthFormat:Le,scaleFactor:o};d=this.getBinding(),f=d.createProjectionLayer(Ge),r.updateRenderState({layers:[f]}),e.setPixelRatio(1),e.setSize(f.textureWidth,f.textureHeight,!1),S=new di(f.textureWidth,f.textureHeight,{format:Ti,type:Yn,depthTexture:new bo(f.textureWidth,f.textureHeight,Xe,void 0,void 0,void 0,void 0,void 0,void 0,ae),stencilBuffer:E.stencil,colorSpace:e.outputColorSpace,samples:E.antialias?4:0,resolveDepthBuffer:f.ignoreDepthValues===!1,resolveStencilBuffer:f.ignoreDepthValues===!1})}else{let ae={antialias:E.antialias,alpha:!0,depth:E.depth,stencil:E.stencil,framebufferScaleFactor:o};h=new XRWebGLLayer(r,t,ae),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),S=new di(h.framebufferWidth,h.framebufferHeight,{format:Ti,type:Yn,colorSpace:e.outputColorSpace,stencilBuffer:E.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}S.isXRRenderTarget=!0,this.setFoveation(c),l=null,s=await r.requestReferenceSpace(a),wt.setContext(r),wt.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return g.getDepthTexture()};function O(X){for(let oe=0;oe<X.removed.length;oe++){let ae=X.removed[oe],Xe=R.indexOf(ae);Xe>=0&&(R[Xe]=null,I[Xe].disconnect(ae))}for(let oe=0;oe<X.added.length;oe++){let ae=X.added[oe],Xe=R.indexOf(ae);if(Xe===-1){for(let Ge=0;Ge<I.length;Ge++)if(Ge>=R.length){R.push(ae),Xe=Ge;break}else if(R[Ge]===null){R[Ge]=ae,Xe=Ge;break}if(Xe===-1)break}let Le=I[Xe];Le&&Le.connect(ae)}}let ee=new T,Z=new T;function fe(X,oe,ae){ee.setFromMatrixPosition(oe.matrixWorld),Z.setFromMatrixPosition(ae.matrixWorld);let Xe=ee.distanceTo(Z),Le=oe.projectionMatrix.elements,Ge=ae.projectionMatrix.elements,$t=Le[14]/(Le[10]-1),ht=Le[14]/(Le[10]+1),gt=(Le[9]+1)/Le[5],Rt=(Le[9]-1)/Le[5],Be=(Le[8]-1)/Le[0],Ht=(Ge[8]+1)/Ge[0],A=$t*Be,Gt=$t*Ht,ft=Xe/(-Be+Ht),ut=ft*-Be;if(oe.matrixWorld.decompose(X.position,X.quaternion,X.scale),X.translateX(ut),X.translateZ(ft),X.matrixWorld.compose(X.position,X.quaternion,X.scale),X.matrixWorldInverse.copy(X.matrixWorld).invert(),Le[10]===-1)X.projectionMatrix.copy(oe.projectionMatrix),X.projectionMatrixInverse.copy(oe.projectionMatrixInverse);else{let be=$t+ft,b=ht+ft,v=A-ut,P=Gt+(Xe-ut),$=gt*ht/b*be,Y=Rt*ht/b*be;X.projectionMatrix.makePerspective(v,P,$,Y,be,b),X.projectionMatrixInverse.copy(X.projectionMatrix).invert()}}function ye(X,oe){oe===null?X.matrixWorld.copy(X.matrix):X.matrixWorld.multiplyMatrices(oe.matrixWorld,X.matrix),X.matrixWorldInverse.copy(X.matrixWorld).invert()}this.updateCamera=function(X){if(r===null)return;let oe=X.near,ae=X.far;g.texture!==null&&(g.depthNear>0&&(oe=g.depthNear),g.depthFar>0&&(ae=g.depthFar)),U.near=q.near=M.near=oe,U.far=q.far=M.far=ae,(B!==U.near||W!==U.far)&&(r.updateRenderState({depthNear:U.near,depthFar:U.far}),B=U.near,W=U.far),U.layers.mask=X.layers.mask|6,M.layers.mask=U.layers.mask&-5,q.layers.mask=U.layers.mask&-3;let Xe=X.parent,Le=U.cameras;ye(U,Xe);for(let Ge=0;Ge<Le.length;Ge++)ye(Le[Ge],Xe);Le.length===2?fe(U,M,q):U.projectionMatrix.copy(M.projectionMatrix),pe(X,U,Xe)};function pe(X,oe,ae){ae===null?X.matrix.copy(oe.matrixWorld):(X.matrix.copy(ae.matrixWorld),X.matrix.invert(),X.matrix.multiply(oe.matrixWorld)),X.matrix.decompose(X.position,X.quaternion,X.scale),X.updateMatrixWorld(!0),X.projectionMatrix.copy(oe.projectionMatrix),X.projectionMatrixInverse.copy(oe.projectionMatrixInverse),X.isPerspectiveCamera&&(X.fov=ka*2*Math.atan(1/X.projectionMatrix.elements[5]),X.zoom=1)}this.getCamera=function(){return U},this.getFoveation=function(){if(!(f===null&&h===null))return c},this.setFoveation=function(X){c=X,f!==null&&(f.fixedFoveation=X),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=X)},this.hasDepthSensing=function(){return g.texture!==null},this.getDepthSensingMesh=function(){return g.getMesh(U)},this.getCameraTexture=function(X){return p[X]};let We=null;function Dt(X,oe){if(u=oe.getViewerPose(l||s),m=oe,u!==null){let ae=u.views;h!==null&&(e.setRenderTargetFramebuffer(S,h.framebuffer),e.setRenderTarget(S));let Xe=!1;ae.length!==U.cameras.length&&(U.cameras.length=0,Xe=!0);for(let ht=0;ht<ae.length;ht++){let gt=ae[ht],Rt=null;if(h!==null)Rt=h.getViewport(gt);else{let Ht=d.getViewSubImage(f,gt);Rt=Ht.viewport,ht===0&&(e.setRenderTargetTextures(S,Ht.colorTexture,Ht.depthStencilTexture),e.setRenderTarget(S))}let Be=C[ht];Be===void 0&&(Be=new On,Be.layers.enable(ht),Be.viewport=new jt,C[ht]=Be),Be.matrix.fromArray(gt.transform.matrix),Be.matrix.decompose(Be.position,Be.quaternion,Be.scale),Be.projectionMatrix.fromArray(gt.projectionMatrix),Be.projectionMatrixInverse.copy(Be.projectionMatrix).invert(),Be.viewport.set(Rt.x,Rt.y,Rt.width,Rt.height),ht===0&&(U.matrix.copy(Be.matrix),U.matrix.decompose(U.position,U.quaternion,U.scale)),Xe===!0&&U.cameras.push(Be)}let Le=r.enabledFeatures;if(Le&&Le.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&y){d=i.getBinding();let ht=d.getDepthInformation(ae[0]);ht&&ht.isValid&&ht.texture&&g.init(ht,r.renderState)}if(Le&&Le.includes("camera-access")&&y){e.state.unbindTexture(),d=i.getBinding();for(let ht=0;ht<ae.length;ht++){let gt=ae[ht].camera;if(gt){let Rt=p[gt];Rt||(Rt=new Ll,p[gt]=Rt);let Be=d.getCameraImage(gt);Rt.sourceTexture=Be}}}}for(let ae=0;ae<I.length;ae++){let Xe=R[ae],Le=I[ae];Xe!==null&&Le!==void 0&&Le.update(Xe,oe,l||s)}We&&We(X,oe),oe.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:oe}),m=null}let wt=new QS;wt.setAnimationLoop(Dt),this.setAnimationLoop=function(X){We=X},this.dispose=function(){}}},Ss=new fr,s2=new Bt;function a2(n,e){function t(g,p){g.matrixAutoUpdate===!0&&g.updateMatrix(),p.value.copy(g.matrix)}function i(g,p){p.color.getRGB(g.fogColor.value,Qy(n)),p.isFog?(g.fogNear.value=p.near,g.fogFar.value=p.far):p.isFogExp2&&(g.fogDensity.value=p.density)}function r(g,p,E,w,S){p.isMeshBasicMaterial?o(g,p):p.isMeshLambertMaterial?(o(g,p),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(o(g,p),d(g,p)):p.isMeshPhongMaterial?(o(g,p),u(g,p),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(o(g,p),f(g,p),p.isMeshPhysicalMaterial&&h(g,p,S)):p.isMeshMatcapMaterial?(o(g,p),m(g,p)):p.isMeshDepthMaterial?o(g,p):p.isMeshDistanceMaterial?(o(g,p),y(g,p)):p.isMeshNormalMaterial?o(g,p):p.isLineBasicMaterial?(s(g,p),p.isLineDashedMaterial&&a(g,p)):p.isPointsMaterial?c(g,p,E,w):p.isSpriteMaterial?l(g,p):p.isShadowMaterial?(g.color.value.copy(p.color),g.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function o(g,p){g.opacity.value=p.opacity,p.color&&g.diffuse.value.copy(p.color),p.emissive&&g.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(g.map.value=p.map,t(p.map,g.mapTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.bumpMap&&(g.bumpMap.value=p.bumpMap,t(p.bumpMap,g.bumpMapTransform),g.bumpScale.value=p.bumpScale,p.side===zn&&(g.bumpScale.value*=-1)),p.normalMap&&(g.normalMap.value=p.normalMap,t(p.normalMap,g.normalMapTransform),g.normalScale.value.copy(p.normalScale),p.side===zn&&g.normalScale.value.negate()),p.displacementMap&&(g.displacementMap.value=p.displacementMap,t(p.displacementMap,g.displacementMapTransform),g.displacementScale.value=p.displacementScale,g.displacementBias.value=p.displacementBias),p.emissiveMap&&(g.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,g.emissiveMapTransform)),p.specularMap&&(g.specularMap.value=p.specularMap,t(p.specularMap,g.specularMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest);let E=e.get(p),w=E.envMap,S=E.envMapRotation;w&&(g.envMap.value=w,Ss.copy(S),Ss.x*=-1,Ss.y*=-1,Ss.z*=-1,w.isCubeTexture&&w.isRenderTargetTexture===!1&&(Ss.y*=-1,Ss.z*=-1),g.envMapRotation.value.setFromMatrix4(s2.makeRotationFromEuler(Ss)),g.flipEnvMap.value=w.isCubeTexture&&w.isRenderTargetTexture===!1?-1:1,g.reflectivity.value=p.reflectivity,g.ior.value=p.ior,g.refractionRatio.value=p.refractionRatio),p.lightMap&&(g.lightMap.value=p.lightMap,g.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,g.lightMapTransform)),p.aoMap&&(g.aoMap.value=p.aoMap,g.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,g.aoMapTransform))}function s(g,p){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,p.map&&(g.map.value=p.map,t(p.map,g.mapTransform))}function a(g,p){g.dashSize.value=p.dashSize,g.totalSize.value=p.dashSize+p.gapSize,g.scale.value=p.scale}function c(g,p,E,w){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,g.size.value=p.size*E,g.scale.value=w*.5,p.map&&(g.map.value=p.map,t(p.map,g.uvTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest)}function l(g,p){g.diffuse.value.copy(p.color),g.opacity.value=p.opacity,g.rotation.value=p.rotation,p.map&&(g.map.value=p.map,t(p.map,g.mapTransform)),p.alphaMap&&(g.alphaMap.value=p.alphaMap,t(p.alphaMap,g.alphaMapTransform)),p.alphaTest>0&&(g.alphaTest.value=p.alphaTest)}function u(g,p){g.specular.value.copy(p.specular),g.shininess.value=Math.max(p.shininess,1e-4)}function d(g,p){p.gradientMap&&(g.gradientMap.value=p.gradientMap)}function f(g,p){g.metalness.value=p.metalness,p.metalnessMap&&(g.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,g.metalnessMapTransform)),g.roughness.value=p.roughness,p.roughnessMap&&(g.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,g.roughnessMapTransform)),p.envMap&&(g.envMapIntensity.value=p.envMapIntensity)}function h(g,p,E){g.ior.value=p.ior,p.sheen>0&&(g.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),g.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(g.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,g.sheenColorMapTransform)),p.sheenRoughnessMap&&(g.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,g.sheenRoughnessMapTransform))),p.clearcoat>0&&(g.clearcoat.value=p.clearcoat,g.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(g.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,g.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(g.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,g.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(g.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,g.clearcoatNormalMapTransform),g.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===zn&&g.clearcoatNormalScale.value.negate())),p.dispersion>0&&(g.dispersion.value=p.dispersion),p.iridescence>0&&(g.iridescence.value=p.iridescence,g.iridescenceIOR.value=p.iridescenceIOR,g.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],g.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(g.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,g.iridescenceMapTransform)),p.iridescenceThicknessMap&&(g.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,g.iridescenceThicknessMapTransform))),p.transmission>0&&(g.transmission.value=p.transmission,g.transmissionSamplerMap.value=E.texture,g.transmissionSamplerSize.value.set(E.width,E.height),p.transmissionMap&&(g.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,g.transmissionMapTransform)),g.thickness.value=p.thickness,p.thicknessMap&&(g.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,g.thicknessMapTransform)),g.attenuationDistance.value=p.attenuationDistance,g.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(g.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(g.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,g.anisotropyMapTransform))),g.specularIntensity.value=p.specularIntensity,g.specularColor.value.copy(p.specularColor),p.specularColorMap&&(g.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,g.specularColorMapTransform)),p.specularIntensityMap&&(g.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,g.specularIntensityMapTransform))}function m(g,p){p.matcap&&(g.matcap.value=p.matcap)}function y(g,p){let E=e.get(p).light;g.referencePosition.value.setFromMatrixPosition(E.matrixWorld),g.nearDistance.value=E.shadow.camera.near,g.farDistance.value=E.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function c2(n,e,t,i){let r={},o={},s=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(E,w){let S=w.program;i.uniformBlockBinding(E,S)}function l(E,w){let S=r[E.id];S===void 0&&(m(E),S=u(E),r[E.id]=S,E.addEventListener("dispose",g));let I=w.program;i.updateUBOMapping(E,I);let R=e.render.frame;o[E.id]!==R&&(f(E),o[E.id]=R)}function u(E){let w=d();E.__bindingPointIndex=w;let S=n.createBuffer(),I=E.__size,R=E.usage;return n.bindBuffer(n.UNIFORM_BUFFER,S),n.bufferData(n.UNIFORM_BUFFER,I,R),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,w,S),S}function d(){for(let E=0;E<a;E++)if(s.indexOf(E)===-1)return s.push(E),E;return ke("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function f(E){let w=r[E.id],S=E.uniforms,I=E.__cache;n.bindBuffer(n.UNIFORM_BUFFER,w);for(let R=0,D=S.length;R<D;R++){let _=Array.isArray(S[R])?S[R]:[S[R]];for(let M=0,q=_.length;M<q;M++){let C=_[M];if(h(C,R,M,I)===!0){let U=C.__offset,B=Array.isArray(C.value)?C.value:[C.value],W=0;for(let H=0;H<B.length;H++){let V=B[H],O=y(V);typeof V=="number"||typeof V=="boolean"?(C.__data[0]=V,n.bufferSubData(n.UNIFORM_BUFFER,U+W,C.__data)):V.isMatrix3?(C.__data[0]=V.elements[0],C.__data[1]=V.elements[1],C.__data[2]=V.elements[2],C.__data[3]=0,C.__data[4]=V.elements[3],C.__data[5]=V.elements[4],C.__data[6]=V.elements[5],C.__data[7]=0,C.__data[8]=V.elements[6],C.__data[9]=V.elements[7],C.__data[10]=V.elements[8],C.__data[11]=0):(V.toArray(C.__data,W),W+=O.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,U,C.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(E,w,S,I){let R=E.value,D=w+"_"+S;if(I[D]===void 0)return typeof R=="number"||typeof R=="boolean"?I[D]=R:I[D]=R.clone(),!0;{let _=I[D];if(typeof R=="number"||typeof R=="boolean"){if(_!==R)return I[D]=R,!0}else if(_.equals(R)===!1)return _.copy(R),!0}return!1}function m(E){let w=E.uniforms,S=0,I=16;for(let D=0,_=w.length;D<_;D++){let M=Array.isArray(w[D])?w[D]:[w[D]];for(let q=0,C=M.length;q<C;q++){let U=M[q],B=Array.isArray(U.value)?U.value:[U.value];for(let W=0,H=B.length;W<H;W++){let V=B[W],O=y(V),ee=S%I,Z=ee%O.boundary,fe=ee+Z;S+=Z,fe!==0&&I-fe<O.storage&&(S+=I-fe),U.__data=new Float32Array(O.storage/Float32Array.BYTES_PER_ELEMENT),U.__offset=S,S+=O.storage}}}let R=S%I;return R>0&&(S+=I-R),E.__size=S,E.__cache={},this}function y(E){let w={boundary:0,storage:0};return typeof E=="number"||typeof E=="boolean"?(w.boundary=4,w.storage=4):E.isVector2?(w.boundary=8,w.storage=8):E.isVector3||E.isColor?(w.boundary=16,w.storage=12):E.isVector4?(w.boundary=16,w.storage=16):E.isMatrix3?(w.boundary=48,w.storage=48):E.isMatrix4?(w.boundary=64,w.storage=64):E.isTexture?Fe("WebGLRenderer: Texture samplers can not be part of an uniforms group."):Fe("WebGLRenderer: Unsupported uniform value type.",E),w}function g(E){let w=E.target;w.removeEventListener("dispose",g);let S=s.indexOf(w.__bindingPointIndex);s.splice(S,1),n.deleteBuffer(r[w.id]),delete r[w.id],delete o[w.id]}function p(){for(let E in r)n.deleteBuffer(r[E]);s=[],r={},o={}}return{bind:c,update:l,dispose:p}}var l2=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),gr=null;function u2(){return gr===null&&(gr=new Xf(l2,16,16,Es,pr),gr.name="DFG_LUT",gr.minFilter=Cn,gr.magFilter=Cn,gr.wrapS=lr,gr.wrapT=lr,gr.generateMipmaps=!1,gr.needsUpdate=!0),gr}var op=class{constructor(e={}){let{canvas:t=wS(),context:i=null,depth:r=!0,stencil:o=!1,alpha:s=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:d=!1,reversedDepthBuffer:f=!1,outputBufferType:h=Yn}=e;this.isWebGLRenderer=!0;let m;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");m=i.getContextAttributes().alpha}else m=s;let y=h,g=new Set([xh,_h,yh]),p=new Set([Yn,Xi,Xa,Ya,gh,vh]),E=new Uint32Array(4),w=new Int32Array(4),S=null,I=null,R=[],D=[],_=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=qi,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let M=this,q=!1;this._outputColorSpace=ai;let C=0,U=0,B=null,W=-1,H=null,V=new jt,O=new jt,ee=null,Z=new Qe(0),fe=0,ye=t.width,pe=t.height,We=1,Dt=null,wt=null,X=new jt(0,0,ye,pe),oe=new jt(0,0,ye,pe),ae=!1,Xe=new Va,Le=!1,Ge=!1,$t=new Bt,ht=new T,gt=new jt,Rt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},Be=!1;function Ht(){return B===null?We:1}let A=i;function Gt(x,L){return t.getContext(x,L)}try{let x={alpha:!0,depth:r,stencil:o,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:d};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"183"}`),t.addEventListener("webglcontextlost",we,!1),t.addEventListener("webglcontextrestored",He,!1),t.addEventListener("webglcontextcreationerror",yt,!1),A===null){let L="webgl2";if(A=Gt(L,x),A===null)throw Gt(L)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(x){throw ke("WebGLRenderer: "+x.message),x}let ft,ut,be,b,v,P,$,Y,G,ge,te,Ae,Pe,K,re,xe,Se,he,tt,N,ie,Q,me;function J(){ft=new yO(A),ft.init(),ie=new i2(A,ft),ut=new uO(A,ft,e,ie),be=new t2(A,ft),ut.reversedDepthBuffer&&f&&be.buffers.depth.setReversed(!0),b=new EO(A),v=new VF,P=new n2(A,ft,be,v,ut,ie,b),$=new vO(M),Y=new TN(A),Q=new cO(A,Y),G=new _O(A,Y,b,Q),ge=new bO(A,G,Y,Q,b),he=new MO(A,ut,P),re=new dO(v),te=new HF(M,$,ft,ut,Q,re),Ae=new a2(M,v),Pe=new GF,K=new YF(ft),Se=new aO(M,$,be,ge,m,c),xe=new e2(M,ge,ut),me=new c2(A,b,ut,be),tt=new lO(A,ft,b),N=new xO(A,ft,b),b.programs=te.programs,M.capabilities=ut,M.extensions=ft,M.properties=v,M.renderLists=Pe,M.shadowMap=xe,M.state=be,M.info=b}J(),y!==Yn&&(_=new wO(y,t.width,t.height,r,o));let j=new v_(M,A);this.xr=j,this.getContext=function(){return A},this.getContextAttributes=function(){return A.getContextAttributes()},this.forceContextLoss=function(){let x=ft.get("WEBGL_lose_context");x&&x.loseContext()},this.forceContextRestore=function(){let x=ft.get("WEBGL_lose_context");x&&x.restoreContext()},this.getPixelRatio=function(){return We},this.setPixelRatio=function(x){x!==void 0&&(We=x,this.setSize(ye,pe,!1))},this.getSize=function(x){return x.set(ye,pe)},this.setSize=function(x,L,z=!0){if(j.isPresenting){Fe("WebGLRenderer: Can't change size while VR device is presenting.");return}ye=x,pe=L,t.width=Math.floor(x*We),t.height=Math.floor(L*We),z===!0&&(t.style.width=x+"px",t.style.height=L+"px"),_!==null&&_.setSize(t.width,t.height),this.setViewport(0,0,x,L)},this.getDrawingBufferSize=function(x){return x.set(ye*We,pe*We).floor()},this.setDrawingBufferSize=function(x,L,z){ye=x,pe=L,We=z,t.width=Math.floor(x*z),t.height=Math.floor(L*z),this.setViewport(0,0,x,L)},this.setEffects=function(x){if(y===Yn){console.error("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(x){for(let L=0;L<x.length;L++)if(x[L].isOutputPass===!0){console.warn("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}_.setEffects(x||[])},this.getCurrentViewport=function(x){return x.copy(V)},this.getViewport=function(x){return x.copy(X)},this.setViewport=function(x,L,z,k){x.isVector4?X.set(x.x,x.y,x.z,x.w):X.set(x,L,z,k),be.viewport(V.copy(X).multiplyScalar(We).round())},this.getScissor=function(x){return x.copy(oe)},this.setScissor=function(x,L,z,k){x.isVector4?oe.set(x.x,x.y,x.z,x.w):oe.set(x,L,z,k),be.scissor(O.copy(oe).multiplyScalar(We).round())},this.getScissorTest=function(){return ae},this.setScissorTest=function(x){be.setScissorTest(ae=x)},this.setOpaqueSort=function(x){Dt=x},this.setTransparentSort=function(x){wt=x},this.getClearColor=function(x){return x.copy(Se.getClearColor())},this.setClearColor=function(){Se.setClearColor(...arguments)},this.getClearAlpha=function(){return Se.getClearAlpha()},this.setClearAlpha=function(){Se.setClearAlpha(...arguments)},this.clear=function(x=!0,L=!0,z=!0){let k=0;if(x){let F=!1;if(B!==null){let ue=B.texture.format;F=g.has(ue)}if(F){let ue=B.texture.type,_e=p.has(ue),le=Se.getClearColor(),Te=Se.getClearAlpha(),Ie=le.r,ze=le.g,qe=le.b;_e?(E[0]=Ie,E[1]=ze,E[2]=qe,E[3]=Te,A.clearBufferuiv(A.COLOR,0,E)):(w[0]=Ie,w[1]=ze,w[2]=qe,w[3]=Te,A.clearBufferiv(A.COLOR,0,w))}else k|=A.COLOR_BUFFER_BIT}L&&(k|=A.DEPTH_BUFFER_BIT),z&&(k|=A.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),k!==0&&A.clear(k)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.dispose=function(){t.removeEventListener("webglcontextlost",we,!1),t.removeEventListener("webglcontextrestored",He,!1),t.removeEventListener("webglcontextcreationerror",yt,!1),Se.dispose(),Pe.dispose(),K.dispose(),v.dispose(),$.dispose(),ge.dispose(),Q.dispose(),me.dispose(),te.dispose(),j.dispose(),j.removeEventListener("sessionstart",rc),j.removeEventListener("sessionend",Br),pi.stop()};function we(x){x.preventDefault(),Zy("WebGLRenderer: Context Lost."),q=!0}function He(){Zy("WebGLRenderer: Context Restored."),q=!1;let x=b.autoReset,L=xe.enabled,z=xe.autoUpdate,k=xe.needsUpdate,F=xe.type;J(),b.autoReset=x,xe.enabled=L,xe.autoUpdate=z,xe.needsUpdate=k,xe.type=F}function yt(x){ke("WebGLRenderer: A WebGL context could not be created. Reason: ",x.statusMessage)}function vt(x){let L=x.target;L.removeEventListener("dispose",vt),Wn(L)}function Wn(x){Ci(x),v.remove(x)}function Ci(x){let L=v.get(x).programs;L!==void 0&&(L.forEach(function(z){te.releaseProgram(z)}),x.isShaderMaterial&&te.releaseShaderCache(x))}this.renderBufferDirect=function(x,L,z,k,F,ue){L===null&&(L=Rt);let _e=F.isMesh&&F.matrixWorld.determinant()<0,le=eu(x,L,z,k,F);be.setMaterial(k,_e);let Te=z.index,Ie=1;if(k.wireframe===!0){if(Te=G.getWireframeAttribute(z),Te===void 0)return;Ie=2}let ze=z.drawRange,qe=z.attributes.position,De=ze.start*Ie,at=(ze.start+ze.count)*Ie;ue!==null&&(De=Math.max(De,ue.start*Ie),at=Math.min(at,(ue.start+ue.count)*Ie)),Te!==null?(De=Math.max(De,0),at=Math.min(at,Te.count)):qe!=null&&(De=Math.max(De,0),at=Math.min(at,qe.count));let Ot=at-De;if(Ot<0||Ot===1/0)return;Q.setup(F,k,le,z,Te);let Ft,Et=tt;if(Te!==null&&(Ft=Y.get(Te),Et=N,Et.setIndex(Ft)),F.isMesh)k.wireframe===!0?(be.setLineWidth(k.wireframeLinewidth*Ht()),Et.setMode(A.LINES)):Et.setMode(A.TRIANGLES);else if(F.isLine){let dn=k.linewidth;dn===void 0&&(dn=1),be.setLineWidth(dn*Ht()),F.isLineSegments?Et.setMode(A.LINES):F.isLineLoop?Et.setMode(A.LINE_LOOP):Et.setMode(A.LINE_STRIP)}else F.isPoints?Et.setMode(A.POINTS):F.isSprite&&Et.setMode(A.TRIANGLES);if(F.isBatchedMesh)if(F._multiDrawInstances!==null)Tl("WebGLRenderer: renderMultiDrawInstances has been deprecated and will be removed in r184. Append to renderMultiDraw arguments and use indirection."),Et.renderMultiDrawInstances(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount,F._multiDrawInstances);else if(ft.get("WEBGL_multi_draw"))Et.renderMultiDraw(F._multiDrawStarts,F._multiDrawCounts,F._multiDrawCount);else{let dn=F._multiDrawStarts,Re=F._multiDrawCounts,kn=F._multiDrawCount,pt=Te?Y.get(Te).bytesPerElement:1,Kn=v.get(k).currentProgram.getUniforms();for(let mi=0;mi<kn;mi++)Kn.setValue(A,"_gl_DrawID",mi),Et.render(dn[mi]/pt,Re[mi])}else if(F.isInstancedMesh)Et.renderInstances(De,Ot,F.count);else if(z.isInstancedBufferGeometry){let dn=z._maxInstanceCount!==void 0?z._maxInstanceCount:1/0,Re=Math.min(z.instanceCount,dn);Et.renderInstances(De,Ot,Re)}else Et.render(De,Ot)};function ic(x,L,z){x.transparent===!0&&x.side===Jt&&x.forceSinglePass===!1?(x.side=zn,x.needsUpdate=!0,Vr(x,L,z),x.side=Or,x.needsUpdate=!0,Vr(x,L,z),x.side=Jt):Vr(x,L,z)}this.compile=function(x,L,z=null){z===null&&(z=x),I=K.get(z),I.init(L),D.push(I),z.traverseVisible(function(F){F.isLight&&F.layers.test(L.layers)&&(I.pushLight(F),F.castShadow&&I.pushShadow(F))}),x!==z&&x.traverseVisible(function(F){F.isLight&&F.layers.test(L.layers)&&(I.pushLight(F),F.castShadow&&I.pushShadow(F))}),I.setupLights();let k=new Set;return x.traverse(function(F){if(!(F.isMesh||F.isPoints||F.isLine||F.isSprite))return;let ue=F.material;if(ue)if(Array.isArray(ue))for(let _e=0;_e<ue.length;_e++){let le=ue[_e];ic(le,z,F),k.add(le)}else ic(ue,z,F),k.add(ue)}),I=D.pop(),k},this.compileAsync=function(x,L,z=null){let k=this.compile(x,L,z);return new Promise(F=>{function ue(){if(k.forEach(function(_e){v.get(_e).currentProgram.isReady()&&k.delete(_e)}),k.size===0){F(x);return}setTimeout(ue,10)}ft.get("KHR_parallel_shader_compile")!==null?ue():setTimeout(ue,10)})};let Ur=null;function _p(x){Ur&&Ur(x)}function rc(){pi.stop()}function Br(){pi.start()}let pi=new QS;pi.setAnimationLoop(_p),typeof self<"u"&&pi.setContext(self),this.setAnimationLoop=function(x){Ur=x,j.setAnimationLoop(x),x===null?pi.stop():pi.start()},j.addEventListener("sessionstart",rc),j.addEventListener("sessionend",Br),this.render=function(x,L){if(L!==void 0&&L.isCamera!==!0){ke("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(q===!0)return;let z=j.enabled===!0&&j.isPresenting===!0,k=_!==null&&(B===null||z)&&_.begin(M,B);if(x.matrixWorldAutoUpdate===!0&&x.updateMatrixWorld(),L.parent===null&&L.matrixWorldAutoUpdate===!0&&L.updateMatrixWorld(),j.enabled===!0&&j.isPresenting===!0&&(_===null||_.isCompositing()===!1)&&(j.cameraAutoUpdate===!0&&j.updateCamera(L),L=j.getCamera()),x.isScene===!0&&x.onBeforeRender(M,x,L,B),I=K.get(x,D.length),I.init(L),D.push(I),$t.multiplyMatrices(L.projectionMatrix,L.matrixWorldInverse),Xe.setFromProjectionMatrix($t,$i,L.reversedDepth),Ge=this.localClippingEnabled,Le=re.init(this.clippingPlanes,Ge),S=Pe.get(x,R.length),S.init(),R.push(S),j.enabled===!0&&j.isPresenting===!0){let _e=M.xr.getDepthSensingMesh();_e!==null&&un(_e,L,-1/0,M.sortObjects)}un(x,L,0,M.sortObjects),S.finish(),M.sortObjects===!0&&S.sort(Dt,wt),Be=j.enabled===!1||j.isPresenting===!1||j.hasDepthSensing()===!1,Be&&Se.addToRenderList(S,x),this.info.render.frame++,Le===!0&&re.beginShadows();let F=I.state.shadowsArray;if(xe.render(F,x,L),Le===!0&&re.endShadows(),this.info.autoReset===!0&&this.info.reset(),(k&&_.hasRenderPass())===!1){let _e=S.opaque,le=S.transmissive;if(I.setupLights(),L.isArrayCamera){let Te=L.cameras;if(le.length>0)for(let Ie=0,ze=Te.length;Ie<ze;Ie++){let qe=Te[Ie];Ts(_e,le,x,qe)}Be&&Se.render(x);for(let Ie=0,ze=Te.length;Ie<ze;Ie++){let qe=Te[Ie];Ii(S,x,qe,qe.viewport)}}else le.length>0&&Ts(_e,le,x,L),Be&&Se.render(x),Ii(S,x,L)}B!==null&&U===0&&(P.updateMultisampleRenderTarget(B),P.updateRenderTargetMipmap(B)),k&&_.end(M),x.isScene===!0&&x.onAfterRender(M,x,L),Q.resetDefaultState(),W=-1,H=null,D.pop(),D.length>0?(I=D[D.length-1],Le===!0&&re.setGlobalState(M.clippingPlanes,I.state.camera)):I=null,R.pop(),R.length>0?S=R[R.length-1]:S=null};function un(x,L,z,k){if(x.visible===!1)return;if(x.layers.test(L.layers)){if(x.isGroup)z=x.renderOrder;else if(x.isLOD)x.autoUpdate===!0&&x.update(L);else if(x.isLight)I.pushLight(x),x.castShadow&&I.pushShadow(x);else if(x.isSprite){if(!x.frustumCulled||Xe.intersectsSprite(x)){k&&gt.setFromMatrixPosition(x.matrixWorld).applyMatrix4($t);let _e=ge.update(x),le=x.material;le.visible&&S.push(x,_e,le,z,gt.z,null)}}else if((x.isMesh||x.isLine||x.isPoints)&&(!x.frustumCulled||Xe.intersectsObject(x))){let _e=ge.update(x),le=x.material;if(k&&(x.boundingSphere!==void 0?(x.boundingSphere===null&&x.computeBoundingSphere(),gt.copy(x.boundingSphere.center)):(_e.boundingSphere===null&&_e.computeBoundingSphere(),gt.copy(_e.boundingSphere.center)),gt.applyMatrix4(x.matrixWorld).applyMatrix4($t)),Array.isArray(le)){let Te=_e.groups;for(let Ie=0,ze=Te.length;Ie<ze;Ie++){let qe=Te[Ie],De=le[qe.materialIndex];De&&De.visible&&S.push(x,_e,De,z,gt.z,qe)}}else le.visible&&S.push(x,_e,le,z,gt.z,null)}}let ue=x.children;for(let _e=0,le=ue.length;_e<le;_e++)un(ue[_e],L,z,k)}function Ii(x,L,z,k){let{opaque:F,transmissive:ue,transparent:_e}=x;I.setupLightsView(z),Le===!0&&re.setGlobalState(M.clippingPlanes,z),k&&be.viewport(V.copy(k)),F.length>0&&Hr(F,L,z),ue.length>0&&Hr(ue,L,z),_e.length>0&&Hr(_e,L,z),be.buffers.depth.setTest(!0),be.buffers.depth.setMask(!0),be.buffers.color.setMask(!0),be.setPolygonOffset(!1)}function Ts(x,L,z,k){if((z.isScene===!0?z.overrideMaterial:null)!==null)return;if(I.state.transmissionRenderTarget[k.id]===void 0){let De=ft.has("EXT_color_buffer_half_float")||ft.has("EXT_color_buffer_float");I.state.transmissionRenderTarget[k.id]=new di(1,1,{generateMipmaps:!0,type:De?pr:Yn,minFilter:Ro,samples:Math.max(4,ut.samples),stencilBuffer:o,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:mt.workingColorSpace})}let ue=I.state.transmissionRenderTarget[k.id],_e=k.viewport||V;ue.setSize(_e.z*M.transmissionResolutionScale,_e.w*M.transmissionResolutionScale);let le=M.getRenderTarget(),Te=M.getActiveCubeFace(),Ie=M.getActiveMipmapLevel();M.setRenderTarget(ue),M.getClearColor(Z),fe=M.getClearAlpha(),fe<1&&M.setClearColor(16777215,.5),M.clear(),Be&&Se.render(z);let ze=M.toneMapping;M.toneMapping=qi;let qe=k.viewport;if(k.viewport!==void 0&&(k.viewport=void 0),I.setupLightsView(k),Le===!0&&re.setGlobalState(M.clippingPlanes,k),Hr(x,z,k),P.updateMultisampleRenderTarget(ue),P.updateRenderTargetMipmap(ue),ft.has("WEBGL_multisampled_render_to_texture")===!1){let De=!1;for(let at=0,Ot=L.length;at<Ot;at++){let Ft=L[at],{object:Et,geometry:dn,material:Re,group:kn}=Ft;if(Re.side===Jt&&Et.layers.test(k.layers)){let pt=Re.side;Re.side=zn,Re.needsUpdate=!0,oc(Et,z,k,dn,Re,kn),Re.side=pt,Re.needsUpdate=!0,De=!0}}De===!0&&(P.updateMultisampleRenderTarget(ue),P.updateRenderTargetMipmap(ue))}M.setRenderTarget(le,Te,Ie),M.setClearColor(Z,fe),qe!==void 0&&(k.viewport=qe),M.toneMapping=ze}function Hr(x,L,z){let k=L.isScene===!0?L.overrideMaterial:null;for(let F=0,ue=x.length;F<ue;F++){let _e=x[F],{object:le,geometry:Te,group:Ie}=_e,ze=_e.material;ze.allowOverride===!0&&k!==null&&(ze=k),le.layers.test(z.layers)&&oc(le,L,z,Te,ze,Ie)}}function oc(x,L,z,k,F,ue){x.onBeforeRender(M,L,z,k,F,ue),x.modelViewMatrix.multiplyMatrices(z.matrixWorldInverse,x.matrixWorld),x.normalMatrix.getNormalMatrix(x.modelViewMatrix),F.onBeforeRender(M,L,z,k,x,ue),F.transparent===!0&&F.side===Jt&&F.forceSinglePass===!1?(F.side=zn,F.needsUpdate=!0,M.renderBufferDirect(z,L,k,F,x,ue),F.side=Or,F.needsUpdate=!0,M.renderBufferDirect(z,L,k,F,x,ue),F.side=Jt):M.renderBufferDirect(z,L,k,F,x,ue),x.onAfterRender(M,L,z,k,F,ue)}function Vr(x,L,z){L.isScene!==!0&&(L=Rt);let k=v.get(x),F=I.state.lights,ue=I.state.shadowsArray,_e=F.state.version,le=te.getParameters(x,F.state,ue,L,z),Te=te.getProgramCacheKey(le),Ie=k.programs;k.environment=x.isMeshStandardMaterial||x.isMeshLambertMaterial||x.isMeshPhongMaterial?L.environment:null,k.fog=L.fog;let ze=x.isMeshStandardMaterial||x.isMeshLambertMaterial&&!x.envMap||x.isMeshPhongMaterial&&!x.envMap;k.envMap=$.get(x.envMap||k.environment,ze),k.envMapRotation=k.environment!==null&&x.envMap===null?L.environmentRotation:x.envMapRotation,Ie===void 0&&(x.addEventListener("dispose",vt),Ie=new Map,k.programs=Ie);let qe=Ie.get(Te);if(qe!==void 0){if(k.currentProgram===qe&&k.lightsStateVersion===_e)return Is(x,le),qe}else le.uniforms=te.getUniforms(x),x.onBeforeCompile(le,M),qe=te.acquireProgram(le,Te),Ie.set(Te,qe),k.uniforms=le.uniforms;let De=k.uniforms;return(!x.isShaderMaterial&&!x.isRawShaderMaterial||x.clipping===!0)&&(De.clippingPlanes=re.uniform),Is(x,le),k.needsLights=yr(x),k.lightsStateVersion=_e,k.needsLights&&(De.ambientLightColor.value=F.state.ambient,De.lightProbe.value=F.state.probe,De.directionalLights.value=F.state.directional,De.directionalLightShadows.value=F.state.directionalShadow,De.spotLights.value=F.state.spot,De.spotLightShadows.value=F.state.spotShadow,De.rectAreaLights.value=F.state.rectArea,De.ltc_1.value=F.state.rectAreaLTC1,De.ltc_2.value=F.state.rectAreaLTC2,De.pointLights.value=F.state.point,De.pointLightShadows.value=F.state.pointShadow,De.hemisphereLights.value=F.state.hemi,De.directionalShadowMatrix.value=F.state.directionalShadowMatrix,De.spotLightMatrix.value=F.state.spotLightMatrix,De.spotLightMap.value=F.state.spotLightMap,De.pointShadowMatrix.value=F.state.pointShadowMatrix),k.currentProgram=qe,k.uniformsList=null,qe}function Cs(x){if(x.uniformsList===null){let L=x.currentProgram.getUniforms();x.uniformsList=Ja.seqWithValue(L.seq,x.uniforms)}return x.uniformsList}function Is(x,L){let z=v.get(x);z.outputColorSpace=L.outputColorSpace,z.batching=L.batching,z.batchingColor=L.batchingColor,z.instancing=L.instancing,z.instancingColor=L.instancingColor,z.instancingMorph=L.instancingMorph,z.skinning=L.skinning,z.morphTargets=L.morphTargets,z.morphNormals=L.morphNormals,z.morphColors=L.morphColors,z.morphTargetsCount=L.morphTargetsCount,z.numClippingPlanes=L.numClippingPlanes,z.numIntersection=L.numClipIntersection,z.vertexAlphas=L.vertexAlphas,z.vertexTangents=L.vertexTangents,z.toneMapping=L.toneMapping}function eu(x,L,z,k,F){L.isScene!==!0&&(L=Rt),P.resetTextureUnits();let ue=L.fog,_e=k.isMeshStandardMaterial||k.isMeshLambertMaterial||k.isMeshPhongMaterial?L.environment:null,le=B===null?M.outputColorSpace:B.isXRRenderTarget===!0?B.texture.colorSpace:ms,Te=k.isMeshStandardMaterial||k.isMeshLambertMaterial&&!k.envMap||k.isMeshPhongMaterial&&!k.envMap,Ie=$.get(k.envMap||_e,Te),ze=k.vertexColors===!0&&!!z.attributes.color&&z.attributes.color.itemSize===4,qe=!!z.attributes.tangent&&(!!k.normalMap||k.anisotropy>0),De=!!z.morphAttributes.position,at=!!z.morphAttributes.normal,Ot=!!z.morphAttributes.color,Ft=qi;k.toneMapped&&(B===null||B.isXRRenderTarget===!0)&&(Ft=M.toneMapping);let Et=z.morphAttributes.position||z.morphAttributes.normal||z.morphAttributes.color,dn=Et!==void 0?Et.length:0,Re=v.get(k),kn=I.state.lights;if(Le===!0&&(Ge===!0||x!==H)){let Zt=x===H&&k.id===W;re.setState(k,x,Zt)}let pt=!1;k.version===Re.__version?(Re.needsLights&&Re.lightsStateVersion!==kn.state.version||Re.outputColorSpace!==le||F.isBatchedMesh&&Re.batching===!1||!F.isBatchedMesh&&Re.batching===!0||F.isBatchedMesh&&Re.batchingColor===!0&&F.colorTexture===null||F.isBatchedMesh&&Re.batchingColor===!1&&F.colorTexture!==null||F.isInstancedMesh&&Re.instancing===!1||!F.isInstancedMesh&&Re.instancing===!0||F.isSkinnedMesh&&Re.skinning===!1||!F.isSkinnedMesh&&Re.skinning===!0||F.isInstancedMesh&&Re.instancingColor===!0&&F.instanceColor===null||F.isInstancedMesh&&Re.instancingColor===!1&&F.instanceColor!==null||F.isInstancedMesh&&Re.instancingMorph===!0&&F.morphTexture===null||F.isInstancedMesh&&Re.instancingMorph===!1&&F.morphTexture!==null||Re.envMap!==Ie||k.fog===!0&&Re.fog!==ue||Re.numClippingPlanes!==void 0&&(Re.numClippingPlanes!==re.numPlanes||Re.numIntersection!==re.numIntersection)||Re.vertexAlphas!==ze||Re.vertexTangents!==qe||Re.morphTargets!==De||Re.morphNormals!==at||Re.morphColors!==Ot||Re.toneMapping!==Ft||Re.morphTargetsCount!==dn)&&(pt=!0):(pt=!0,Re.__version=k.version);let Kn=Re.currentProgram;pt===!0&&(Kn=Vr(k,L,F));let mi=!1,Ai=!1,Zi=!1,_t=Kn.getUniforms(),fn=Re.uniforms;if(be.useProgram(Kn.program)&&(mi=!0,Ai=!0,Zi=!0),k.id!==W&&(W=k.id,Ai=!0),mi||H!==x){be.buffers.depth.getReversed()&&x.reversedDepth!==!0&&(x._reversedDepth=!0,x.updateProjectionMatrix()),_t.setValue(A,"projectionMatrix",x.projectionMatrix),_t.setValue(A,"viewMatrix",x.matrixWorldInverse);let Ri=_t.map.cameraPosition;Ri!==void 0&&Ri.setValue(A,ht.setFromMatrixPosition(x.matrixWorld)),ut.logarithmicDepthBuffer&&_t.setValue(A,"logDepthBufFC",2/(Math.log(x.far+1)/Math.LN2)),(k.isMeshPhongMaterial||k.isMeshToonMaterial||k.isMeshLambertMaterial||k.isMeshBasicMaterial||k.isMeshStandardMaterial||k.isShaderMaterial)&&_t.setValue(A,"isOrthographic",x.isOrthographicCamera===!0),H!==x&&(H=x,Ai=!0,Zi=!0)}if(Re.needsLights&&(kn.state.directionalShadowMap.length>0&&_t.setValue(A,"directionalShadowMap",kn.state.directionalShadowMap,P),kn.state.spotShadowMap.length>0&&_t.setValue(A,"spotShadowMap",kn.state.spotShadowMap,P),kn.state.pointShadowMap.length>0&&_t.setValue(A,"pointShadowMap",kn.state.pointShadowMap,P)),F.isSkinnedMesh){_t.setOptional(A,F,"bindMatrix"),_t.setOptional(A,F,"bindMatrixInverse");let Zt=F.skeleton;Zt&&(Zt.boneTexture===null&&Zt.computeBoneTexture(),_t.setValue(A,"boneTexture",Zt.boneTexture,P))}F.isBatchedMesh&&(_t.setOptional(A,F,"batchingTexture"),_t.setValue(A,"batchingTexture",F._matricesTexture,P),_t.setOptional(A,F,"batchingIdTexture"),_t.setValue(A,"batchingIdTexture",F._indirectTexture,P),_t.setOptional(A,F,"batchingColorTexture"),F._colorsTexture!==null&&_t.setValue(A,"batchingColorTexture",F._colorsTexture,P));let Jn=z.morphAttributes;if((Jn.position!==void 0||Jn.normal!==void 0||Jn.color!==void 0)&&he.update(F,z,Kn),(Ai||Re.receiveShadow!==F.receiveShadow)&&(Re.receiveShadow=F.receiveShadow,_t.setValue(A,"receiveShadow",F.receiveShadow)),(k.isMeshStandardMaterial||k.isMeshLambertMaterial||k.isMeshPhongMaterial)&&k.envMap===null&&L.environment!==null&&(fn.envMapIntensity.value=L.environmentIntensity),fn.dfgLUT!==void 0&&(fn.dfgLUT.value=u2()),Ai&&(_t.setValue(A,"toneMappingExposure",M.toneMappingExposure),Re.needsLights&&sc(fn,Zi),ue&&k.fog===!0&&Ae.refreshFogUniforms(fn,ue),Ae.refreshMaterialUniforms(fn,k,We,pe,I.state.transmissionRenderTarget[x.id]),Ja.upload(A,Cs(Re),fn,P)),k.isShaderMaterial&&k.uniformsNeedUpdate===!0&&(Ja.upload(A,Cs(Re),fn,P),k.uniformsNeedUpdate=!1),k.isSpriteMaterial&&_t.setValue(A,"center",F.center),_t.setValue(A,"modelViewMatrix",F.modelViewMatrix),_t.setValue(A,"normalMatrix",F.normalMatrix),_t.setValue(A,"modelMatrix",F.matrixWorld),k.isShaderMaterial||k.isRawShaderMaterial){let Zt=k.uniformsGroups;for(let Ri=0,Di=Zt.length;Ri<Di;Ri++){let Ce=Zt[Ri];me.update(Ce,Kn),me.bind(Ce,Kn)}}return Kn}function sc(x,L){x.ambientLightColor.needsUpdate=L,x.lightProbe.needsUpdate=L,x.directionalLights.needsUpdate=L,x.directionalLightShadows.needsUpdate=L,x.pointLights.needsUpdate=L,x.pointLightShadows.needsUpdate=L,x.spotLights.needsUpdate=L,x.spotLightShadows.needsUpdate=L,x.rectAreaLights.needsUpdate=L,x.hemisphereLights.needsUpdate=L}function yr(x){return x.isMeshLambertMaterial||x.isMeshToonMaterial||x.isMeshPhongMaterial||x.isMeshStandardMaterial||x.isShadowMaterial||x.isShaderMaterial&&x.lights===!0}this.getActiveCubeFace=function(){return C},this.getActiveMipmapLevel=function(){return U},this.getRenderTarget=function(){return B},this.setRenderTargetTextures=function(x,L,z){let k=v.get(x);k.__autoAllocateDepthBuffer=x.resolveDepthBuffer===!1,k.__autoAllocateDepthBuffer===!1&&(k.__useRenderToTexture=!1),v.get(x.texture).__webglTexture=L,v.get(x.depthTexture).__webglTexture=k.__autoAllocateDepthBuffer?void 0:z,k.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(x,L){let z=v.get(x);z.__webglFramebuffer=L,z.__useDefaultFramebuffer=L===void 0};let xp=A.createFramebuffer();this.setRenderTarget=function(x,L=0,z=0){B=x,C=L,U=z;let k=null,F=!1,ue=!1;if(x){let le=v.get(x);if(le.__useDefaultFramebuffer!==void 0){be.bindFramebuffer(A.FRAMEBUFFER,le.__webglFramebuffer),V.copy(x.viewport),O.copy(x.scissor),ee=x.scissorTest,be.viewport(V),be.scissor(O),be.setScissorTest(ee),W=-1;return}else if(le.__webglFramebuffer===void 0)P.setupRenderTarget(x);else if(le.__hasExternalTextures)P.rebindTextures(x,v.get(x.texture).__webglTexture,v.get(x.depthTexture).__webglTexture);else if(x.depthBuffer){let ze=x.depthTexture;if(le.__boundDepthTexture!==ze){if(ze!==null&&v.has(ze)&&(x.width!==ze.image.width||x.height!==ze.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");P.setupDepthRenderbuffer(x)}}let Te=x.texture;(Te.isData3DTexture||Te.isDataArrayTexture||Te.isCompressedArrayTexture)&&(ue=!0);let Ie=v.get(x).__webglFramebuffer;x.isWebGLCubeRenderTarget?(Array.isArray(Ie[L])?k=Ie[L][z]:k=Ie[L],F=!0):x.samples>0&&P.useMultisampledRTT(x)===!1?k=v.get(x).__webglMultisampledFramebuffer:Array.isArray(Ie)?k=Ie[z]:k=Ie,V.copy(x.viewport),O.copy(x.scissor),ee=x.scissorTest}else V.copy(X).multiplyScalar(We).floor(),O.copy(oe).multiplyScalar(We).floor(),ee=ae;if(z!==0&&(k=xp),be.bindFramebuffer(A.FRAMEBUFFER,k)&&be.drawBuffers(x,k),be.viewport(V),be.scissor(O),be.setScissorTest(ee),F){let le=v.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_CUBE_MAP_POSITIVE_X+L,le.__webglTexture,z)}else if(ue){let le=L;for(let Te=0;Te<x.textures.length;Te++){let Ie=v.get(x.textures[Te]);A.framebufferTextureLayer(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0+Te,Ie.__webglTexture,z,le)}}else if(x!==null&&z!==0){let le=v.get(x.texture);A.framebufferTexture2D(A.FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,le.__webglTexture,z)}W=-1},this.readRenderTargetPixels=function(x,L,z,k,F,ue,_e,le=0){if(!(x&&x.isWebGLRenderTarget)){ke("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let Te=v.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(Te=Te[_e]),Te){be.bindFramebuffer(A.FRAMEBUFFER,Te);try{let Ie=x.textures[le],ze=Ie.format,qe=Ie.type;if(x.textures.length>1&&A.readBuffer(A.COLOR_ATTACHMENT0+le),!ut.textureFormatReadable(ze)){ke("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!ut.textureTypeReadable(qe)){ke("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}L>=0&&L<=x.width-k&&z>=0&&z<=x.height-F&&A.readPixels(L,z,k,F,ie.convert(ze),ie.convert(qe),ue)}finally{let Ie=B!==null?v.get(B).__webglFramebuffer:null;be.bindFramebuffer(A.FRAMEBUFFER,Ie)}}},this.readRenderTargetPixelsAsync=async function(x,L,z,k,F,ue,_e,le=0){if(!(x&&x.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let Te=v.get(x).__webglFramebuffer;if(x.isWebGLCubeRenderTarget&&_e!==void 0&&(Te=Te[_e]),Te)if(L>=0&&L<=x.width-k&&z>=0&&z<=x.height-F){be.bindFramebuffer(A.FRAMEBUFFER,Te);let Ie=x.textures[le],ze=Ie.format,qe=Ie.type;if(x.textures.length>1&&A.readBuffer(A.COLOR_ATTACHMENT0+le),!ut.textureFormatReadable(ze))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!ut.textureTypeReadable(qe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let De=A.createBuffer();A.bindBuffer(A.PIXEL_PACK_BUFFER,De),A.bufferData(A.PIXEL_PACK_BUFFER,ue.byteLength,A.STREAM_READ),A.readPixels(L,z,k,F,ie.convert(ze),ie.convert(qe),0);let at=B!==null?v.get(B).__webglFramebuffer:null;be.bindFramebuffer(A.FRAMEBUFFER,at);let Ot=A.fenceSync(A.SYNC_GPU_COMMANDS_COMPLETE,0);return A.flush(),await CS(A,Ot,4),A.bindBuffer(A.PIXEL_PACK_BUFFER,De),A.getBufferSubData(A.PIXEL_PACK_BUFFER,0,ue),A.deleteBuffer(De),A.deleteSync(Ot),ue}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(x,L=null,z=0){let k=Math.pow(2,-z),F=Math.floor(x.image.width*k),ue=Math.floor(x.image.height*k),_e=L!==null?L.x:0,le=L!==null?L.y:0;P.setTexture2D(x,0),A.copyTexSubImage2D(A.TEXTURE_2D,z,0,0,_e,le,F,ue),be.unbindTexture()};let tu=A.createFramebuffer(),nu=A.createFramebuffer();this.copyTextureToTexture=function(x,L,z=null,k=null,F=0,ue=0){let _e,le,Te,Ie,ze,qe,De,at,Ot,Ft=x.isCompressedTexture?x.mipmaps[ue]:x.image;if(z!==null)_e=z.max.x-z.min.x,le=z.max.y-z.min.y,Te=z.isBox3?z.max.z-z.min.z:1,Ie=z.min.x,ze=z.min.y,qe=z.isBox3?z.min.z:0;else{let fn=Math.pow(2,-F);_e=Math.floor(Ft.width*fn),le=Math.floor(Ft.height*fn),x.isDataArrayTexture?Te=Ft.depth:x.isData3DTexture?Te=Math.floor(Ft.depth*fn):Te=1,Ie=0,ze=0,qe=0}k!==null?(De=k.x,at=k.y,Ot=k.z):(De=0,at=0,Ot=0);let Et=ie.convert(L.format),dn=ie.convert(L.type),Re;L.isData3DTexture?(P.setTexture3D(L,0),Re=A.TEXTURE_3D):L.isDataArrayTexture||L.isCompressedArrayTexture?(P.setTexture2DArray(L,0),Re=A.TEXTURE_2D_ARRAY):(P.setTexture2D(L,0),Re=A.TEXTURE_2D),A.pixelStorei(A.UNPACK_FLIP_Y_WEBGL,L.flipY),A.pixelStorei(A.UNPACK_PREMULTIPLY_ALPHA_WEBGL,L.premultiplyAlpha),A.pixelStorei(A.UNPACK_ALIGNMENT,L.unpackAlignment);let kn=A.getParameter(A.UNPACK_ROW_LENGTH),pt=A.getParameter(A.UNPACK_IMAGE_HEIGHT),Kn=A.getParameter(A.UNPACK_SKIP_PIXELS),mi=A.getParameter(A.UNPACK_SKIP_ROWS),Ai=A.getParameter(A.UNPACK_SKIP_IMAGES);A.pixelStorei(A.UNPACK_ROW_LENGTH,Ft.width),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,Ft.height),A.pixelStorei(A.UNPACK_SKIP_PIXELS,Ie),A.pixelStorei(A.UNPACK_SKIP_ROWS,ze),A.pixelStorei(A.UNPACK_SKIP_IMAGES,qe);let Zi=x.isDataArrayTexture||x.isData3DTexture,_t=L.isDataArrayTexture||L.isData3DTexture;if(x.isDepthTexture){let fn=v.get(x),Jn=v.get(L),Zt=v.get(fn.__renderTarget),Ri=v.get(Jn.__renderTarget);be.bindFramebuffer(A.READ_FRAMEBUFFER,Zt.__webglFramebuffer),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,Ri.__webglFramebuffer);for(let Di=0;Di<Te;Di++)Zi&&(A.framebufferTextureLayer(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,v.get(x).__webglTexture,F,qe+Di),A.framebufferTextureLayer(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,v.get(L).__webglTexture,ue,Ot+Di)),A.blitFramebuffer(Ie,ze,_e,le,De,at,_e,le,A.DEPTH_BUFFER_BIT,A.NEAREST);be.bindFramebuffer(A.READ_FRAMEBUFFER,null),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,null)}else if(F!==0||x.isRenderTargetTexture||v.has(x)){let fn=v.get(x),Jn=v.get(L);be.bindFramebuffer(A.READ_FRAMEBUFFER,tu),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,nu);for(let Zt=0;Zt<Te;Zt++)Zi?A.framebufferTextureLayer(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,fn.__webglTexture,F,qe+Zt):A.framebufferTexture2D(A.READ_FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,fn.__webglTexture,F),_t?A.framebufferTextureLayer(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,Jn.__webglTexture,ue,Ot+Zt):A.framebufferTexture2D(A.DRAW_FRAMEBUFFER,A.COLOR_ATTACHMENT0,A.TEXTURE_2D,Jn.__webglTexture,ue),F!==0?A.blitFramebuffer(Ie,ze,_e,le,De,at,_e,le,A.COLOR_BUFFER_BIT,A.NEAREST):_t?A.copyTexSubImage3D(Re,ue,De,at,Ot+Zt,Ie,ze,_e,le):A.copyTexSubImage2D(Re,ue,De,at,Ie,ze,_e,le);be.bindFramebuffer(A.READ_FRAMEBUFFER,null),be.bindFramebuffer(A.DRAW_FRAMEBUFFER,null)}else _t?x.isDataTexture||x.isData3DTexture?A.texSubImage3D(Re,ue,De,at,Ot,_e,le,Te,Et,dn,Ft.data):L.isCompressedArrayTexture?A.compressedTexSubImage3D(Re,ue,De,at,Ot,_e,le,Te,Et,Ft.data):A.texSubImage3D(Re,ue,De,at,Ot,_e,le,Te,Et,dn,Ft):x.isDataTexture?A.texSubImage2D(A.TEXTURE_2D,ue,De,at,_e,le,Et,dn,Ft.data):x.isCompressedTexture?A.compressedTexSubImage2D(A.TEXTURE_2D,ue,De,at,Ft.width,Ft.height,Et,Ft.data):A.texSubImage2D(A.TEXTURE_2D,ue,De,at,_e,le,Et,dn,Ft);A.pixelStorei(A.UNPACK_ROW_LENGTH,kn),A.pixelStorei(A.UNPACK_IMAGE_HEIGHT,pt),A.pixelStorei(A.UNPACK_SKIP_PIXELS,Kn),A.pixelStorei(A.UNPACK_SKIP_ROWS,mi),A.pixelStorei(A.UNPACK_SKIP_IMAGES,Ai),ue===0&&L.generateMipmaps&&A.generateMipmap(Re),be.unbindTexture()},this.initRenderTarget=function(x){v.get(x).__webglFramebuffer===void 0&&P.setupRenderTarget(x)},this.initTexture=function(x){x.isCubeTexture?P.setTextureCube(x,0):x.isData3DTexture?P.setTexture3D(x,0):x.isDataArrayTexture||x.isCompressedArrayTexture?P.setTexture2DArray(x,0):P.setTexture2D(x,0),be.unbindTexture()},this.resetState=function(){C=0,U=0,B=null,be.reset(),Q.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return $i}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=mt._getDrawingBufferColorSpace(e),t.unpackColorSpace=mt._getUnpackColorSpace()}};var rw={type:"change"},x_={type:"start"},sw={type:"end"},cp=new Eo,ow=new wi,d2=Math.cos(70*Jy.DEG2RAD),cn=new T,Zn=2*Math.PI,Lt={NONE:-1,ROTATE:0,DOLLY:1,PAN:2,TOUCH_ROTATE:3,TOUCH_PAN:4,TOUCH_DOLLY_PAN:5,TOUCH_DOLLY_ROTATE:6},__=1e-6,lp=class extends _s{constructor(e,t=null){super(e,t),this.state=Lt.NONE,this.target=new T,this.cursor=new T,this.minDistance=0,this.maxDistance=1/0,this.minZoom=0,this.maxZoom=1/0,this.minTargetRadius=0,this.maxTargetRadius=1/0,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.minAzimuthAngle=-1/0,this.maxAzimuthAngle=1/0,this.enableDamping=!1,this.dampingFactor=.05,this.enableZoom=!0,this.zoomSpeed=1,this.enableRotate=!0,this.rotateSpeed=1,this.keyRotateSpeed=1,this.enablePan=!0,this.panSpeed=1,this.screenSpacePanning=!0,this.keyPanSpeed=7,this.zoomToCursor=!1,this.autoRotate=!1,this.autoRotateSpeed=2,this.keys={LEFT:"ArrowLeft",UP:"ArrowUp",RIGHT:"ArrowRight",BOTTOM:"ArrowDown"},this.mouseButtons={LEFT:Co.ROTATE,MIDDLE:Co.DOLLY,RIGHT:Co.PAN},this.touches={ONE:Io.ROTATE,TWO:Io.DOLLY_PAN},this.target0=this.target.clone(),this.position0=this.object.position.clone(),this.zoom0=this.object.zoom,this._cursorStyle="auto",this._domElementKeyEvents=null,this._lastPosition=new T,this._lastQuaternion=new ui,this._lastTargetPosition=new T,this._quat=new ui().setFromUnitVectors(e.up,new T(0,1,0)),this._quatInverse=this._quat.clone().invert(),this._spherical=new $a,this._sphericalDelta=new $a,this._scale=1,this._panOffset=new T,this._rotateStart=new Ue,this._rotateEnd=new Ue,this._rotateDelta=new Ue,this._panStart=new Ue,this._panEnd=new Ue,this._panDelta=new Ue,this._dollyStart=new Ue,this._dollyEnd=new Ue,this._dollyDelta=new Ue,this._dollyDirection=new T,this._mouse=new Ue,this._performCursorZoom=!1,this._pointers=[],this._pointerPositions={},this._controlActive=!1,this._onPointerMove=h2.bind(this),this._onPointerDown=f2.bind(this),this._onPointerUp=p2.bind(this),this._onContextMenu=E2.bind(this),this._onMouseWheel=v2.bind(this),this._onKeyDown=y2.bind(this),this._onTouchStart=_2.bind(this),this._onTouchMove=x2.bind(this),this._onMouseDown=m2.bind(this),this._onMouseMove=g2.bind(this),this._interceptControlDown=M2.bind(this),this._interceptControlUp=b2.bind(this),this.domElement!==null&&this.connect(this.domElement),this.update()}set cursorStyle(e){this._cursorStyle=e,e==="grab"?this.domElement.style.cursor="grab":this.domElement.style.cursor="auto"}get cursorStyle(){return this._cursorStyle}connect(e){super.connect(e),this.domElement.addEventListener("pointerdown",this._onPointerDown),this.domElement.addEventListener("pointercancel",this._onPointerUp),this.domElement.addEventListener("contextmenu",this._onContextMenu),this.domElement.addEventListener("wheel",this._onMouseWheel,{passive:!1}),this.domElement.getRootNode().addEventListener("keydown",this._interceptControlDown,{passive:!0,capture:!0}),this.domElement.style.touchAction="none"}disconnect(){this.domElement.removeEventListener("pointerdown",this._onPointerDown),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.domElement.removeEventListener("pointercancel",this._onPointerUp),this.domElement.removeEventListener("wheel",this._onMouseWheel),this.domElement.removeEventListener("contextmenu",this._onContextMenu),this.stopListenToKeyEvents(),this.domElement.getRootNode().removeEventListener("keydown",this._interceptControlDown,{capture:!0}),this.domElement.style.touchAction="auto"}dispose(){this.disconnect()}getPolarAngle(){return this._spherical.phi}getAzimuthalAngle(){return this._spherical.theta}getDistance(){return this.object.position.distanceTo(this.target)}listenToKeyEvents(e){e.addEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=e}stopListenToKeyEvents(){this._domElementKeyEvents!==null&&(this._domElementKeyEvents.removeEventListener("keydown",this._onKeyDown),this._domElementKeyEvents=null)}saveState(){this.target0.copy(this.target),this.position0.copy(this.object.position),this.zoom0=this.object.zoom}reset(){this.target.copy(this.target0),this.object.position.copy(this.position0),this.object.zoom=this.zoom0,this.object.updateProjectionMatrix(),this.dispatchEvent(rw),this.update(),this.state=Lt.NONE}pan(e,t){this._pan(e,t),this.update()}dollyIn(e){this._dollyIn(e),this.update()}dollyOut(e){this._dollyOut(e),this.update()}rotateLeft(e){this._rotateLeft(e),this.update()}rotateUp(e){this._rotateUp(e),this.update()}update(e=null){let t=this.object.position;cn.copy(t).sub(this.target),cn.applyQuaternion(this._quat),this._spherical.setFromVector3(cn),this.autoRotate&&this.state===Lt.NONE&&this._rotateLeft(this._getAutoRotationAngle(e)),this.enableDamping?(this._spherical.theta+=this._sphericalDelta.theta*this.dampingFactor,this._spherical.phi+=this._sphericalDelta.phi*this.dampingFactor):(this._spherical.theta+=this._sphericalDelta.theta,this._spherical.phi+=this._sphericalDelta.phi);let i=this.minAzimuthAngle,r=this.maxAzimuthAngle;isFinite(i)&&isFinite(r)&&(i<-Math.PI?i+=Zn:i>Math.PI&&(i-=Zn),r<-Math.PI?r+=Zn:r>Math.PI&&(r-=Zn),i<=r?this._spherical.theta=Math.max(i,Math.min(r,this._spherical.theta)):this._spherical.theta=this._spherical.theta>(i+r)/2?Math.max(i,this._spherical.theta):Math.min(r,this._spherical.theta)),this._spherical.phi=Math.max(this.minPolarAngle,Math.min(this.maxPolarAngle,this._spherical.phi)),this._spherical.makeSafe(),this.enableDamping===!0?this.target.addScaledVector(this._panOffset,this.dampingFactor):this.target.add(this._panOffset),this.target.sub(this.cursor),this.target.clampLength(this.minTargetRadius,this.maxTargetRadius),this.target.add(this.cursor);let o=!1;if(this.zoomToCursor&&this._performCursorZoom||this.object.isOrthographicCamera)this._spherical.radius=this._clampDistance(this._spherical.radius);else{let s=this._spherical.radius;this._spherical.radius=this._clampDistance(this._spherical.radius*this._scale),o=s!=this._spherical.radius}if(cn.setFromSpherical(this._spherical),cn.applyQuaternion(this._quatInverse),t.copy(this.target).add(cn),this.object.lookAt(this.target),this.enableDamping===!0?(this._sphericalDelta.theta*=1-this.dampingFactor,this._sphericalDelta.phi*=1-this.dampingFactor,this._panOffset.multiplyScalar(1-this.dampingFactor)):(this._sphericalDelta.set(0,0,0),this._panOffset.set(0,0,0)),this.zoomToCursor&&this._performCursorZoom){let s=null;if(this.object.isPerspectiveCamera){let a=cn.length();s=this._clampDistance(a*this._scale);let c=a-s;this.object.position.addScaledVector(this._dollyDirection,c),this.object.updateMatrixWorld(),o=!!c}else if(this.object.isOrthographicCamera){let a=new T(this._mouse.x,this._mouse.y,0);a.unproject(this.object);let c=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),this.object.updateProjectionMatrix(),o=c!==this.object.zoom;let l=new T(this._mouse.x,this._mouse.y,0);l.unproject(this.object),this.object.position.sub(l).add(a),this.object.updateMatrixWorld(),s=cn.length()}else console.warn("WARNING: OrbitControls.js encountered an unknown camera type - zoom to cursor disabled."),this.zoomToCursor=!1;s!==null&&(this.screenSpacePanning?this.target.set(0,0,-1).transformDirection(this.object.matrix).multiplyScalar(s).add(this.object.position):(cp.origin.copy(this.object.position),cp.direction.set(0,0,-1).transformDirection(this.object.matrix),Math.abs(this.object.up.dot(cp.direction))<d2?this.object.lookAt(this.target):(ow.setFromNormalAndCoplanarPoint(this.object.up,this.target),cp.intersectPlane(ow,this.target))))}else if(this.object.isOrthographicCamera){let s=this.object.zoom;this.object.zoom=Math.max(this.minZoom,Math.min(this.maxZoom,this.object.zoom/this._scale)),s!==this.object.zoom&&(this.object.updateProjectionMatrix(),o=!0)}return this._scale=1,this._performCursorZoom=!1,o||this._lastPosition.distanceToSquared(this.object.position)>__||8*(1-this._lastQuaternion.dot(this.object.quaternion))>__||this._lastTargetPosition.distanceToSquared(this.target)>__?(this.dispatchEvent(rw),this._lastPosition.copy(this.object.position),this._lastQuaternion.copy(this.object.quaternion),this._lastTargetPosition.copy(this.target),!0):!1}_getAutoRotationAngle(e){return e!==null?Zn/60*this.autoRotateSpeed*e:Zn/60/60*this.autoRotateSpeed}_getZoomScale(e){let t=Math.abs(e*.01);return Math.pow(.95,this.zoomSpeed*t)}_rotateLeft(e){this._sphericalDelta.theta-=e}_rotateUp(e){this._sphericalDelta.phi-=e}_panLeft(e,t){cn.setFromMatrixColumn(t,0),cn.multiplyScalar(-e),this._panOffset.add(cn)}_panUp(e,t){this.screenSpacePanning===!0?cn.setFromMatrixColumn(t,1):(cn.setFromMatrixColumn(t,0),cn.crossVectors(this.object.up,cn)),cn.multiplyScalar(e),this._panOffset.add(cn)}_pan(e,t){let i=this.domElement;if(this.object.isPerspectiveCamera){let r=this.object.position;cn.copy(r).sub(this.target);let o=cn.length();o*=Math.tan(this.object.fov/2*Math.PI/180),this._panLeft(2*e*o/i.clientHeight,this.object.matrix),this._panUp(2*t*o/i.clientHeight,this.object.matrix)}else this.object.isOrthographicCamera?(this._panLeft(e*(this.object.right-this.object.left)/this.object.zoom/i.clientWidth,this.object.matrix),this._panUp(t*(this.object.top-this.object.bottom)/this.object.zoom/i.clientHeight,this.object.matrix)):(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - pan disabled."),this.enablePan=!1)}_dollyOut(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale/=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_dollyIn(e){this.object.isPerspectiveCamera||this.object.isOrthographicCamera?this._scale*=e:(console.warn("WARNING: OrbitControls.js encountered an unknown camera type - dolly/zoom disabled."),this.enableZoom=!1)}_updateZoomParameters(e,t){if(!this.zoomToCursor)return;this._performCursorZoom=!0;let i=this.domElement.getBoundingClientRect(),r=e-i.left,o=t-i.top,s=i.width,a=i.height;this._mouse.x=r/s*2-1,this._mouse.y=-(o/a)*2+1,this._dollyDirection.set(this._mouse.x,this._mouse.y,1).unproject(this.object).sub(this.object.position).normalize()}_clampDistance(e){return Math.max(this.minDistance,Math.min(this.maxDistance,e))}_handleMouseDownRotate(e){this._rotateStart.set(e.clientX,e.clientY)}_handleMouseDownDolly(e){this._updateZoomParameters(e.clientX,e.clientX),this._dollyStart.set(e.clientX,e.clientY)}_handleMouseDownPan(e){this._panStart.set(e.clientX,e.clientY)}_handleMouseMoveRotate(e){this._rotateEnd.set(e.clientX,e.clientY),this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(Zn*this._rotateDelta.x/t.clientHeight),this._rotateUp(Zn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd),this.update()}_handleMouseMoveDolly(e){this._dollyEnd.set(e.clientX,e.clientY),this._dollyDelta.subVectors(this._dollyEnd,this._dollyStart),this._dollyDelta.y>0?this._dollyOut(this._getZoomScale(this._dollyDelta.y)):this._dollyDelta.y<0&&this._dollyIn(this._getZoomScale(this._dollyDelta.y)),this._dollyStart.copy(this._dollyEnd),this.update()}_handleMouseMovePan(e){this._panEnd.set(e.clientX,e.clientY),this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd),this.update()}_handleMouseWheel(e){this._updateZoomParameters(e.clientX,e.clientY),e.deltaY<0?this._dollyIn(this._getZoomScale(e.deltaY)):e.deltaY>0&&this._dollyOut(this._getZoomScale(e.deltaY)),this.update()}_handleKeyDown(e){let t=!1;switch(e.code){case this.keys.UP:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(Zn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,this.keyPanSpeed),t=!0;break;case this.keys.BOTTOM:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateUp(-Zn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(0,-this.keyPanSpeed),t=!0;break;case this.keys.LEFT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(Zn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(this.keyPanSpeed,0),t=!0;break;case this.keys.RIGHT:e.ctrlKey||e.metaKey||e.shiftKey?this.enableRotate&&this._rotateLeft(-Zn*this.keyRotateSpeed/this.domElement.clientHeight):this.enablePan&&this._pan(-this.keyPanSpeed,0),t=!0;break}t&&(e.preventDefault(),this.update())}_handleTouchStartRotate(e){if(this._pointers.length===1)this._rotateStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._rotateStart.set(i,r)}}_handleTouchStartPan(e){if(this._pointers.length===1)this._panStart.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panStart.set(i,r)}}_handleTouchStartDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,o=Math.sqrt(i*i+r*r);this._dollyStart.set(0,o)}_handleTouchStartDollyPan(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enablePan&&this._handleTouchStartPan(e)}_handleTouchStartDollyRotate(e){this.enableZoom&&this._handleTouchStartDolly(e),this.enableRotate&&this._handleTouchStartRotate(e)}_handleTouchMoveRotate(e){if(this._pointers.length==1)this._rotateEnd.set(e.pageX,e.pageY);else{let i=this._getSecondPointerPosition(e),r=.5*(e.pageX+i.x),o=.5*(e.pageY+i.y);this._rotateEnd.set(r,o)}this._rotateDelta.subVectors(this._rotateEnd,this._rotateStart).multiplyScalar(this.rotateSpeed);let t=this.domElement;this._rotateLeft(Zn*this._rotateDelta.x/t.clientHeight),this._rotateUp(Zn*this._rotateDelta.y/t.clientHeight),this._rotateStart.copy(this._rotateEnd)}_handleTouchMovePan(e){if(this._pointers.length===1)this._panEnd.set(e.pageX,e.pageY);else{let t=this._getSecondPointerPosition(e),i=.5*(e.pageX+t.x),r=.5*(e.pageY+t.y);this._panEnd.set(i,r)}this._panDelta.subVectors(this._panEnd,this._panStart).multiplyScalar(this.panSpeed),this._pan(this._panDelta.x,this._panDelta.y),this._panStart.copy(this._panEnd)}_handleTouchMoveDolly(e){let t=this._getSecondPointerPosition(e),i=e.pageX-t.x,r=e.pageY-t.y,o=Math.sqrt(i*i+r*r);this._dollyEnd.set(0,o),this._dollyDelta.set(0,Math.pow(this._dollyEnd.y/this._dollyStart.y,this.zoomSpeed)),this._dollyOut(this._dollyDelta.y),this._dollyStart.copy(this._dollyEnd);let s=(e.pageX+t.x)*.5,a=(e.pageY+t.y)*.5;this._updateZoomParameters(s,a)}_handleTouchMoveDollyPan(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enablePan&&this._handleTouchMovePan(e)}_handleTouchMoveDollyRotate(e){this.enableZoom&&this._handleTouchMoveDolly(e),this.enableRotate&&this._handleTouchMoveRotate(e)}_addPointer(e){this._pointers.push(e.pointerId)}_removePointer(e){delete this._pointerPositions[e.pointerId];for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId){this._pointers.splice(t,1);return}}_isTrackingPointer(e){for(let t=0;t<this._pointers.length;t++)if(this._pointers[t]==e.pointerId)return!0;return!1}_trackPointer(e){let t=this._pointerPositions[e.pointerId];t===void 0&&(t=new Ue,this._pointerPositions[e.pointerId]=t),t.set(e.pageX,e.pageY)}_getSecondPointerPosition(e){let t=e.pointerId===this._pointers[0]?this._pointers[1]:this._pointers[0];return this._pointerPositions[t]}_customWheelEvent(e){let t=e.deltaMode,i={clientX:e.clientX,clientY:e.clientY,deltaY:e.deltaY};switch(t){case 1:i.deltaY*=16;break;case 2:i.deltaY*=100;break}return e.ctrlKey&&!this._controlActive&&(i.deltaY*=10),i}};function f2(n){this.enabled!==!1&&(this._pointers.length===0&&(this.domElement.setPointerCapture(n.pointerId),this.domElement.ownerDocument.addEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.addEventListener("pointerup",this._onPointerUp)),!this._isTrackingPointer(n)&&(this._addPointer(n),n.pointerType==="touch"?this._onTouchStart(n):this._onMouseDown(n),this._cursorStyle==="grab"&&(this.domElement.style.cursor="grabbing")))}function h2(n){this.enabled!==!1&&(n.pointerType==="touch"?this._onTouchMove(n):this._onMouseMove(n))}function p2(n){switch(this._removePointer(n),this._pointers.length){case 0:this.domElement.releasePointerCapture(n.pointerId),this.domElement.ownerDocument.removeEventListener("pointermove",this._onPointerMove),this.domElement.ownerDocument.removeEventListener("pointerup",this._onPointerUp),this.dispatchEvent(sw),this.state=Lt.NONE,this._cursorStyle==="grab"&&(this.domElement.style.cursor="grab");break;case 1:let e=this._pointers[0],t=this._pointerPositions[e];this._onTouchStart({pointerId:e,pageX:t.x,pageY:t.y});break}}function m2(n){let e;switch(n.button){case 0:e=this.mouseButtons.LEFT;break;case 1:e=this.mouseButtons.MIDDLE;break;case 2:e=this.mouseButtons.RIGHT;break;default:e=-1}switch(e){case Co.DOLLY:if(this.enableZoom===!1)return;this._handleMouseDownDolly(n),this.state=Lt.DOLLY;break;case Co.ROTATE:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=Lt.PAN}else{if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=Lt.ROTATE}break;case Co.PAN:if(n.ctrlKey||n.metaKey||n.shiftKey){if(this.enableRotate===!1)return;this._handleMouseDownRotate(n),this.state=Lt.ROTATE}else{if(this.enablePan===!1)return;this._handleMouseDownPan(n),this.state=Lt.PAN}break;default:this.state=Lt.NONE}this.state!==Lt.NONE&&this.dispatchEvent(x_)}function g2(n){switch(this.state){case Lt.ROTATE:if(this.enableRotate===!1)return;this._handleMouseMoveRotate(n);break;case Lt.DOLLY:if(this.enableZoom===!1)return;this._handleMouseMoveDolly(n);break;case Lt.PAN:if(this.enablePan===!1)return;this._handleMouseMovePan(n);break}}function v2(n){this.enabled===!1||this.enableZoom===!1||this.state!==Lt.NONE||(n.preventDefault(),this.dispatchEvent(x_),this._handleMouseWheel(this._customWheelEvent(n)),this.dispatchEvent(sw))}function y2(n){this.enabled!==!1&&this._handleKeyDown(n)}function _2(n){switch(this._trackPointer(n),this._pointers.length){case 1:switch(this.touches.ONE){case Io.ROTATE:if(this.enableRotate===!1)return;this._handleTouchStartRotate(n),this.state=Lt.TOUCH_ROTATE;break;case Io.PAN:if(this.enablePan===!1)return;this._handleTouchStartPan(n),this.state=Lt.TOUCH_PAN;break;default:this.state=Lt.NONE}break;case 2:switch(this.touches.TWO){case Io.DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchStartDollyPan(n),this.state=Lt.TOUCH_DOLLY_PAN;break;case Io.DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchStartDollyRotate(n),this.state=Lt.TOUCH_DOLLY_ROTATE;break;default:this.state=Lt.NONE}break;default:this.state=Lt.NONE}this.state!==Lt.NONE&&this.dispatchEvent(x_)}function x2(n){switch(this._trackPointer(n),this.state){case Lt.TOUCH_ROTATE:if(this.enableRotate===!1)return;this._handleTouchMoveRotate(n),this.update();break;case Lt.TOUCH_PAN:if(this.enablePan===!1)return;this._handleTouchMovePan(n),this.update();break;case Lt.TOUCH_DOLLY_PAN:if(this.enableZoom===!1&&this.enablePan===!1)return;this._handleTouchMoveDollyPan(n),this.update();break;case Lt.TOUCH_DOLLY_ROTATE:if(this.enableZoom===!1&&this.enableRotate===!1)return;this._handleTouchMoveDollyRotate(n),this.update();break;default:this.state=Lt.NONE}}function E2(n){this.enabled!==!1&&n.preventDefault()}function M2(n){n.key==="Control"&&(this._controlActive=!0,this.domElement.getRootNode().addEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}function b2(n){n.key==="Control"&&(this._controlActive=!1,this.domElement.getRootNode().removeEventListener("keyup",this._interceptControlUp,{passive:!0,capture:!0}))}var ec=new fr(0,0,0,"YXZ"),tc=new T,S2={type:"change"},w2={type:"lock"},T2={type:"unlock"},aw=.002,cw=Math.PI/2,up=class extends _s{constructor(e,t=null){super(e,t),this.isLocked=!1,this.minPolarAngle=0,this.maxPolarAngle=Math.PI,this.pointerSpeed=1,this._onMouseMove=C2.bind(this),this._onPointerlockChange=I2.bind(this),this._onPointerlockError=A2.bind(this),this.domElement!==null&&this.connect(this.domElement)}connect(e){super.connect(e),this.domElement.ownerDocument.addEventListener("mousemove",this._onMouseMove),this.domElement.ownerDocument.addEventListener("pointerlockchange",this._onPointerlockChange),this.domElement.ownerDocument.addEventListener("pointerlockerror",this._onPointerlockError)}disconnect(){this.domElement.ownerDocument.removeEventListener("mousemove",this._onMouseMove),this.domElement.ownerDocument.removeEventListener("pointerlockchange",this._onPointerlockChange),this.domElement.ownerDocument.removeEventListener("pointerlockerror",this._onPointerlockError)}dispose(){this.disconnect()}getDirection(e){return e.set(0,0,-1).applyQuaternion(this.object.quaternion)}moveForward(e){if(this.enabled===!1)return;let t=this.object;tc.setFromMatrixColumn(t.matrix,0),tc.crossVectors(t.up,tc),t.position.addScaledVector(tc,e)}moveRight(e){if(this.enabled===!1)return;let t=this.object;tc.setFromMatrixColumn(t.matrix,0),t.position.addScaledVector(tc,e)}lock(e=!1){this.domElement.requestPointerLock({unadjustedMovement:e})}unlock(){this.domElement.ownerDocument.exitPointerLock()}};function C2(n){if(this.enabled===!1||this.isLocked===!1)return;let e=this.object;ec.setFromQuaternion(e.quaternion),ec.y-=n.movementX*aw*this.pointerSpeed,ec.x-=n.movementY*aw*this.pointerSpeed,ec.x=Math.max(cw-this.maxPolarAngle,Math.min(cw-this.minPolarAngle,ec.x)),e.quaternion.setFromEuler(ec),this.dispatchEvent(S2)}function I2(){this.domElement.ownerDocument.pointerLockElement===this.domElement?(this.dispatchEvent(w2),this.isLocked=!0):(this.dispatchEvent(T2),this.isLocked=!1)}function A2(){console.error("THREE.PointerLockControls: Unable to use Pointer Lock API")}var Jl=class extends Xn{constructor(e=document.createElement("div")){super(),this.isCSS2DObject=!0,this.element=e,this.element.style.position="absolute",this.element.style.userSelect="none",this.element.setAttribute("draggable",!1),this.center=new Ue(.5,.5),this.addEventListener("removed",function(){this.traverse(function(t){t.element&&t.element instanceof t.element.ownerDocument.defaultView.Element&&t.element.parentNode!==null&&t.element.remove()})})}copy(e,t){return super.copy(e,t),this.element=e.element.cloneNode(!0),this.center=e.center,this}},nc=new T,lw=new Bt,uw=new Bt,dw=new T,fw=new T,dp=class{constructor(e={}){let t=this,i,r,o,s,a={objects:new WeakMap},c=e.element!==void 0?e.element:document.createElement("div");c.style.overflow="hidden",this.domElement=c,this.sortObjects=!0,this.getSize=function(){return{width:i,height:r}},this.render=function(m,y){m.matrixWorldAutoUpdate===!0&&m.updateMatrixWorld(),y.parent===null&&y.matrixWorldAutoUpdate===!0&&y.updateMatrixWorld(),lw.copy(y.matrixWorldInverse),uw.multiplyMatrices(y.projectionMatrix,lw),u(m,m,y),this.sortObjects&&h(m)},this.setSize=function(m,y){i=m,r=y,o=i/2,s=r/2,c.style.width=m+"px",c.style.height=y+"px"};function l(m){m.isCSS2DObject&&(m.element.style.display="none");for(let y=0,g=m.children.length;y<g;y++)l(m.children[y])}function u(m,y,g){if(m.visible===!1){l(m);return}if(m.isCSS2DObject){nc.setFromMatrixPosition(m.matrixWorld),nc.applyMatrix4(uw);let p=nc.z>=-1&&nc.z<=1&&m.layers.test(g.layers)===!0,E=m.element;E.style.display=p===!0?"":"none",p===!0&&(m.onBeforeRender(t,y,g),E.style.transform="translate("+-100*m.center.x+"%,"+-100*m.center.y+"%)translate("+(nc.x*o+o)+"px,"+(-nc.y*s+s)+"px)",E.parentNode!==c&&c.appendChild(E),m.onAfterRender(t,y,g));let w={distanceToCameraSquared:d(g,m)};a.objects.set(m,w)}for(let p=0,E=m.children.length;p<E;p++)u(m.children[p],y,g)}function d(m,y){return dw.setFromMatrixPosition(m.matrixWorld),fw.setFromMatrixPosition(y.matrixWorld),dw.distanceToSquared(fw)}function f(m){let y=[];return m.traverseVisible(function(g){g.isCSS2DObject&&y.push(g)}),y}function h(m){let y=f(m).sort(function(p,E){if(p.renderOrder!==E.renderOrder)return E.renderOrder-p.renderOrder;let w=a.objects.get(p).distanceToCameraSquared,S=a.objects.get(E).distanceToCameraSquared;return w-S}),g=y.length;for(let p=0,E=y.length;p<E;p++)y[p].element.style.zIndex=g-p}}};function R2(){let n=document.createElement("canvas");n.width=512,n.height=512;let e=n.getContext("2d"),t=64,i=24,r=3,o=Math.ceil(n.height/(i+r)),s=Math.ceil(n.width/(t+r))+1;e.fillStyle="#8a8580",e.fillRect(0,0,n.width,n.height);let a=["#a0522d","#8b4513","#b5651d","#9e5c3a","#c4703f","#7a4422","#a86032","#bf7040","#8d5533","#b06830","#6e3b1e","#c97d4a","#94583a","#ab6940","#7f4828","#be7545"];for(let l=0;l<o;l++){let u=l*(i+r),d=l%2===0?0:-(t/2+r/2);for(let f=-1;f<s;f++){let h=f*(t+r)+d,m=a[Math.floor(Math.random()*a.length)];e.fillStyle=m,e.fillRect(h,u,t,i);for(let y=0;y<15;y++){let g=h+Math.random()*t,p=u+Math.random()*i,E=Math.random()>.5?"rgba(255,255,255,0.06)":"rgba(0,0,0,0.08)";e.fillStyle=E,e.fillRect(g,p,Math.random()*12+2,Math.random()*4+1)}}}let c=new Mo(n);return c.wrapS=li,c.wrapT=li,c}function D2(){let n=document.createElement("canvas");n.width=512,n.height=512;let e=n.getContext("2d"),t=64,i=24,r=3,o=Math.ceil(n.height/(i+r)),s=Math.ceil(n.width/(t+r))+1;e.fillStyle="#000000",e.fillRect(0,0,n.width,n.height);for(let c=0;c<o;c++){let l=c*(i+r),u=c%2===0?0:-(t/2+r/2);for(let d=-1;d<s;d++){let f=d*(t+r)+u,h=180+Math.floor(Math.random()*40);e.fillStyle=`rgb(${h},${h},${h})`,e.fillRect(f,l,t,i)}}let a=new Mo(n);return a.wrapS=li,a.wrapT=li,a}function N2(){let n=document.createElement("canvas");n.width=256,n.height=256;let e=n.getContext("2d");e.fillStyle="#4a8c3f",e.fillRect(0,0,256,256);for(let i=0;i<5e3;i++){let r=Math.random()*256,o=Math.random()*256,s=Math.floor(100+Math.random()*80);e.fillStyle=`rgb(${Math.floor(s*.5)},${s},${Math.floor(s*.3)})`,e.fillRect(r,o,2,2)}let t=new Mo(n);return t.wrapS=li,t.wrapT=li,t.repeat.set(20,20),t}function P2(){let n=document.createElement("canvas");n.width=256,n.height=256;let e=n.getContext("2d");e.fillStyle="#b8956a",e.fillRect(0,0,256,256);for(let i=0;i<256;i+=2){let r=160+Math.sin(i*.3)*20+Math.random()*15;e.fillStyle=`rgb(${Math.floor(r)},${Math.floor(r*.7)},${Math.floor(r*.4)})`,e.fillRect(0,i,256,1)}for(let i=0;i<256;i+=32)e.fillStyle="rgba(0,0,0,0.1)",e.fillRect(i,0,1,256);let t=new Mo(n);return t.wrapS=li,t.wrapT=li,t.repeat.set(4,4),t}function hw(){let n=R2(),e=D2();return{brick:(t=3,i=2)=>{let r=n.clone();r.repeat.set(t,i),r.needsUpdate=!0;let o=e.clone();return o.repeat.set(t,i),o.needsUpdate=!0,new St({map:r,bumpMap:o,bumpScale:.04,metalness:0,roughness:.85,side:Jt})},glass:new za({color:8965375,metalness:.1,roughness:.05,transparent:!0,opacity:.25,side:Jt}),steelFrame:new St({color:1710618,metalness:.8,roughness:.3}),steelColumn:new St({color:4473924,metalness:.7,roughness:.35}),concrete:new St({color:10066329,metalness:0,roughness:.9}),grass:new St({map:N2(),metalness:0,roughness:.95}),roofPanel:new St({color:7020576,metalness:.4,roughness:.5,side:Jt}),roofTop:new St({color:2763306,metalness:.1,roughness:.8,side:Jt}),roofWood:new St({color:9133628,metalness:0,roughness:.75,side:Jt}),roofFascia:new St({color:3815994,metalness:.2,roughness:.6}),interiorWall:new St({color:16117992,metalness:0,roughness:.9,side:Jt}),interiorFloor:new St({map:P2(),metalness:.1,roughness:.7}),deckTile:new St({color:12687979,metalness:.05,roughness:.75,side:Jt}),deckRailing:new St({color:3355443,metalness:.7,roughness:.3}),railingGlass:new za({color:13426158,metalness:0,roughness:.1,transparent:!0,opacity:.15,side:Jt}),steppingStone:new St({color:13417369,metalness:0,roughness:.8}),gardenSoil:new St({color:5913899,metalness:0,roughness:1}),gardenWall:new St({color:6710886,metalness:0,roughness:.9}),lattice:new St({color:12886894,metalness:0,roughness:.8})}}var L2=12.192,O2=2.438,E_=2.591,st=L2*.75,Ne=O2*1.5,ln=E_,Gn=st/3.5,Oo=1.5*Gn,xn=Gn,Yt=Gn+Oo,M_=[{name:"Bedroom 1",bounds:{minX:0,maxX:xn,minZ:0,maxZ:Ne},labelPos:new T(Gn/2,1.5,Ne/2),description:"Bedroom with window view",area:`${(Gn*Ne).toFixed(1)} m\xB2`,color:2201331},{name:"Living Room",bounds:{minX:xn,maxX:Yt,minZ:0,maxZ:Ne},labelPos:new T((xn+Yt)/2,1.5,Ne/2),description:"Open-plan living area with sliding glass door to veranda",area:`${(Oo*Ne).toFixed(1)} m\xB2`,color:5025616},{name:"Bedroom 2",bounds:{minX:Yt,maxX:st,minZ:0,maxZ:Ne},labelPos:new T((Yt+st)/2,1.5,Ne/2),description:"Master bedroom with panoramic windows",area:`${(Gn*Ne).toFixed(1)} m\xB2`,color:10233776}],Po=(3.3875-.9125)*.75,F2=2.05-.55,Ql=.7*(Oo-.6),pw=2.3,mw=.4*.75,k2=2.1-.3,fp=Gn/2,hp=xn;var gw=Yt+Gn/2,pp=(xn+Yt)/2,vw=[{start:new T(0,0,-.5),end:new T(st,0,-.5),label:`${st.toFixed(2)}m`,offset:new T(0,.3,0)},{start:new T(-.5,0,0),end:new T(-.5,0,Ne),label:`${Ne.toFixed(2)}m`,offset:new T(0,.3,0)},{start:new T(st+.3,0,0),end:new T(st+.3,E_,0),label:`${E_.toFixed(2)}m`,offset:new T(.3,0,0)},{start:new T(fp-Po/2,2.15,-.3),end:new T(fp+Po/2,2.15,-.3),label:`${Po.toFixed(2)}m`,offset:new T(0,.2,0),color:"#1565c0"},{start:new T(fp-Po/2-.15,.55,-.3),end:new T(fp-Po/2-.15,2.05,-.3),label:`${F2.toFixed(2)}m`,offset:new T(-.25,0,0),color:"#1565c0"},{start:new T(pp-Ql/2,2.4,-.3),end:new T(pp+Ql/2,2.4,-.3),label:`${Ql.toFixed(2)}m`,offset:new T(0,.2,0),color:"#2e7d32"},{start:new T(pp+Ql/2+.15,0,-.3),end:new T(pp+Ql/2+.15,pw,-.3),label:`${pw.toFixed(2)}m`,offset:new T(.25,0,0),color:"#2e7d32"},{start:new T(gw-Po/2,2.15,-.3),end:new T(gw+Po/2,2.15,-.3),label:`${Po.toFixed(2)}m`,offset:new T(0,.2,0),color:"#1565c0"},{start:new T(hp+.5,2.2,Ne+.3),end:new T(hp+.5+mw,2.2,Ne+.3),label:`${mw.toFixed(2)}m`,offset:new T(0,.2,0),color:"#1565c0"},{start:new T(hp+.3,.3,Ne+.3),end:new T(hp+.3,2.1,Ne+.3),label:`${k2.toFixed(2)}m`,offset:new T(-.2,0,0),color:"#1565c0"},{start:new T(0,.2,.3),end:new T(Gn,.2,.3),label:`Bedroom 1: ${Gn.toFixed(2)}m`,offset:new T(0,.4,0),color:"#2196f3"},{start:new T(.3,.2,0),end:new T(.3,.2,Ne),label:`${Ne.toFixed(2)}m`,offset:new T(-.3,.4,0),color:"#2196f3"},{start:new T(xn,.2,.3),end:new T(Yt,.2,.3),label:`Living Room: ${Oo.toFixed(2)}m`,offset:new T(0,.4,0),color:"#4caf50"},{start:new T(xn+.3,.2,0),end:new T(xn+.3,.2,Ne),label:`${Ne.toFixed(2)}m`,offset:new T(-.3,.4,0),color:"#4caf50"},{start:new T(Yt,.2,.3),end:new T(st,.2,.3),label:`Bedroom 2: ${Gn.toFixed(2)}m`,offset:new T(0,.4,0),color:"#9c27b0"},{start:new T(Yt+.3,.2,0),end:new T(Yt+.3,.2,Ne),label:`${Ne.toFixed(2)}m`,offset:new T(-.3,.4,0),color:"#9c27b0"}];function U2(n,e,t){let i=new Set([0,e]),r=new Set([0,n]);for(let c of t)i.add(c.bottom),i.add(c.top),r.add(c.left),r.add(c.right);let o=Array.from(i).sort((c,l)=>c-l),s=Array.from(r).sort((c,l)=>c-l),a=[];for(let c=0;c<o.length-1;c++){let l=o[c],u=o[c+1],d=u-l;for(let f=0;f<s.length-1;f++){let h=s[f],m=s[f+1],y=m-h;if(y<.001||d<.001)continue;t.some(p=>h>=p.left-.001&&m<=p.right+.001&&l>=p.bottom-.001&&u<=p.top+.001)||a.push({x:(h+m)/2,y:(l+u)/2,w:y,h:d})}}return a}function mp(n,e,t,i,r,o,s=[]){let a=new zt,c=U2(n,e,t);for(let u of c){let d=new Wt(u.w,u.h),f=new Me(d,i);f.position.set(u.x-n/2,u.y,0),f.castShadow=!0,f.receiveShadow=!0,a.add(f)}let l=.05;for(let u=0;u<t.length;u++){let d=t[u];if(s.includes(u))continue;let f=d.right-d.left,h=d.top-d.bottom,m=(d.left+d.right)/2-n/2,y=(d.bottom+d.top)/2,g=new Me(new Wt(f-.04,h-.04),r);g.position.set(m,y,.005),a.add(g);let p=new Me(new ct(f+l*2,l,l),o);p.position.set(m,d.top,0),p.castShadow=!0,a.add(p);let E=new Me(new ct(f+l*2,l,l),o);E.position.set(m,d.bottom,0),E.castShadow=!0,a.add(E);let w=new Me(new ct(l,h,l),o);w.position.set(d.left-n/2,y,0),w.castShadow=!0,a.add(w);let S=new Me(new ct(l,h,l),o);if(S.position.set(d.right-n/2,y,0),S.castShadow=!0,a.add(S),f>1.8){let I=new Me(new ct(l*.6,h,l),o);I.position.set(m,y,0),a.add(I)}if(h>1.5){let I=new Me(new ct(f,l*.6,l),o);I.position.set(m,y,0),a.add(I)}}return a}function Lo(n,e,t,i){let r=new zt,o=.05,s=new Me(new Wt(n-o,e-o),t);s.position.set(n/2,e/2,.005),r.add(s);let a=new Me(new ct(n,o,o),i);a.position.set(n/2,e-o/2,0),a.castShadow=!0,r.add(a);let c=new Me(new ct(n,o,o),i);c.position.set(n/2,o/2,0),c.castShadow=!0,r.add(c);let l=new Me(new ct(o,e,o),i);l.position.set(o/2,e/2,0),l.castShadow=!0,r.add(l);let u=new Me(new ct(o,e,o),i);if(u.position.set(n-o/2,e/2,0),u.castShadow=!0,r.add(u),e>1){let d=new Me(new ct(n-o,o*.6,o),i);d.position.set(n/2,e/2,0),r.add(d)}return r}function yw(){let n=new zt,e=hw(),t=e.brick(6,3),i=e.brick(2,3),r=e.brick(6,3),o=new zt,s=new Me(new ct(st+.4,.15,Ne+.4),e.concrete);s.position.set(st/2,-.075,Ne/2),s.receiveShadow=!0,o.add(s),n.add(o);let a=(3.3875-.9125)*.75,c=.55,l=2.05,u=Gn/2,d=Yt+Gn/2,f=[{left:u-a/2,right:u+a/2,bottom:c,top:l},{left:(xn+Yt)/2-.7*(Oo-.6)/2,right:(xn+Yt)/2+.7*(Oo-.6)/2,bottom:0,top:2.3},{left:d-a/2,right:d+a/2,bottom:c,top:l}],h=mp(st,ln,f,t,e.glass,e.steelFrame,[0,1,2]);h.rotation.y=Math.PI,h.position.set(st/2,0,0),n.add(h);let m=new St({color:2899536,metalness:.6,roughness:.35,side:Jt}),y=new zt,g=(xn+Yt)/2-.7*(Oo-.6)/2,p=(xn+Yt)/2+.7*(Oo-.6)/2,E=0,w=2.3,S=p-g,I=w-E,R=(g+p)/2,D=-.02,_=.055,M=.05,q=.07,C=new Me(new ct(S+q,q,M),m);C.position.set(R,w,D),C.castShadow=!0,y.add(C);let U=S/4,B=.4,W=Math.PI*.4,H=1.85-B,V=I-1.85,O=Ce=>{let lt=new zt;for(let en of[0,Ce]){let _r=new Me(new ct(_,I,M),m);_r.position.set(en,I/2,0),_r.castShadow=!0,lt.add(_r)}for(let en of[0,B,1.85,I]){let _r=new Me(new ct(Ce,_,M),m);_r.position.set(Ce/2,en,0),_r.castShadow=!0,lt.add(_r)}let xt=new Me(new Wt(Ce-_,B-_),m);xt.position.set(Ce/2,B/2,.005),lt.add(xt);let kt=new Me(new Wt(Ce-_-.02,H-_-.02),e.glass);kt.position.set(Ce/2,B+H/2,.005),lt.add(kt);let Qt=new Me(new Wt(Ce-_-.02,V-_-.02),e.glass);return Qt.position.set(Ce/2,1.85+V/2,.005),lt.add(Qt),lt},ee=O(U);ee.position.set(g,0,D),y.add(ee);let Z=O(U);Z.scale.x=-1,Z.position.set(p,0,D),y.add(Z);let fe=O(U);fe.position.set(g+U,0,D),fe.rotation.y=W,y.add(fe);let ye=O(U);ye.scale.x=-1,ye.position.set(p-U,0,D),ye.rotation.y=-W,y.add(ye),n.add(y),n.userData.doorLeftLeaf=fe,n.userData.doorRightLeaf=ye,n.userData.doorOpenAngle=W,n.userData.doorIsOpen=!0;let pe=.4*.75,We=.3,Dt=2.1,wt=.5,X=[{left:xn+wt,right:xn+wt+pe,bottom:We,top:Dt},{left:Yt-wt-pe,right:Yt-wt,bottom:We,top:Dt}],oe=mp(st,ln,X,r,e.glass,e.steelFrame);oe.position.set(st/2,0,Ne),n.add(oe);let ae=e.steelFrame,Xe=.04,Le=Dt-We;for(let Ce of X){let lt=(Ce.left+Ce.right)/2,xt=Ce.right-Ce.left;for(let kt=1;kt<=3;kt++){let Qt=We+Le/4*kt,en=new Me(new ct(xt,Xe,.04),ae);en.position.set(lt,Qt,Ne+.01),n.add(en)}}let $t=mp(Ne,ln,[{left:.6844,right:2.5406,bottom:.55,top:2.05}],i,e.glass,e.steelFrame,[0]);$t.rotation.y=Math.PI/2,$t.position.set(0,0,Ne/2),n.add($t);let gt=mp(Ne,ln,[{left:.6844,right:2.5406,bottom:.55,top:2.05}],i,e.glass,e.steelFrame,[0]);gt.rotation.y=-Math.PI/2,gt.position.set(st,0,Ne/2),n.add(gt);let Rt=Math.PI*.35,Be=l-c,Ht=a/2,A=.6844,Gt=2.5406,ft=(Gt-A)/2,ut=[];{let Ce=new zt;Ce.position.set(u-a/2,c,0),n.add(Ce);let lt=Lo(Ht,Be,e.glass,e.steelFrame);Ce.add(lt),ut.push({leaf:lt,sign:1});let xt=new zt;xt.position.set(u+a/2,c,0),n.add(xt);let kt=Lo(Ht,Be,e.glass,e.steelFrame);kt.scale.x=-1,xt.add(kt),ut.push({leaf:kt,sign:-1})}{let Ce=new zt;Ce.position.set(d-a/2,c,0),n.add(Ce);let lt=Lo(Ht,Be,e.glass,e.steelFrame);Ce.add(lt),ut.push({leaf:lt,sign:1});let xt=new zt;xt.position.set(d+a/2,c,0),n.add(xt);let kt=Lo(Ht,Be,e.glass,e.steelFrame);kt.scale.x=-1,xt.add(kt),ut.push({leaf:kt,sign:-1})}{let Ce=Ne-A,lt=Ne-Gt,xt=new zt;xt.position.set(0,c,Ce),xt.rotation.y=Math.PI/2,n.add(xt);let kt=Lo(ft,Be,e.glass,e.steelFrame);xt.add(kt),ut.push({leaf:kt,sign:1});let Qt=new zt;Qt.position.set(0,c,lt),Qt.rotation.y=-Math.PI/2,n.add(Qt);let en=Lo(ft,Be,e.glass,e.steelFrame);Qt.add(en),ut.push({leaf:en,sign:-1})}{let Ce=Gt,lt=A,xt=new zt;xt.position.set(st,c,Ce),xt.rotation.y=Math.PI/2,n.add(xt);let kt=Lo(ft,Be,e.glass,e.steelFrame);xt.add(kt),ut.push({leaf:kt,sign:-1});let Qt=new zt;Qt.position.set(st,c,lt),Qt.rotation.y=-Math.PI/2,n.add(Qt);let en=Lo(ft,Be,e.glass,e.steelFrame);Qt.add(en),ut.push({leaf:en,sign:1})}n.userData.windowLeaves=ut,n.userData.windowOpenAngle=Rt;let be=new zt,b=.32,v=.4,P=.6,$=.6,Y=.3,G=.14,ge=-P,te=st+$,Ae=-b,Pe=Ne+v,K=te-ge,re=Pe-Ae,xe=ln+.1+Y,Se=ln+.1,he=new _n,tt=new Float32Array([ge,xe,Ae,te,xe,Ae,te,Se,Pe,ge,Se,Pe]);he.setAttribute("position",new vn(tt,3)),he.setIndex([0,2,1,0,3,2]),he.computeVertexNormals();let N=new Me(he,e.roofTop);N.castShadow=!0,N.receiveShadow=!0,be.add(N);let ie=xe-G,Q=Se-G,me=new _n,J=new Float32Array([ge,ie,Ae,te,ie,Ae,te,Q,Pe,ge,Q,Pe]);me.setAttribute("position",new vn(J,3)),me.setIndex([0,1,2,0,2,3]),me.computeVertexNormals();let j=new Me(me,e.roofWood);be.add(j);let we=new Me(new ct(K,G,.05),e.roofFascia);we.position.set((ge+te)/2,xe-G/2,Ae),we.castShadow=!0,be.add(we);let He=new Me(new ct(K,G,.05),e.roofFascia);He.position.set((ge+te)/2,Se-G/2,Pe),He.castShadow=!0,be.add(He);let yt=ie-ln;if(yt>.01){let Ce=new Me(new Wt(st,yt),t);Ce.rotation.y=Math.PI,Ce.position.set(st/2,ln+yt/2,0),Ce.castShadow=!0,n.add(Ce);let lt=new Me(new Wt(st,yt),e.interiorWall);lt.position.set(st/2,ln+yt/2,.01),n.add(lt)}let vt=Q-ln;if(vt>.01){let Ce=new Me(new Wt(st,vt),r);Ce.position.set(st/2,ln+vt/2,Ne),Ce.castShadow=!0,n.add(Ce)}let Wn=new _n,Ci=new Float32Array([0,ln,0,0,ie,0,0,Q,Ne,0,ln,Ne]);Wn.setAttribute("position",new vn(Ci,3)),Wn.setIndex([0,1,2,0,2,3]),Wn.computeVertexNormals();let ic=new Me(Wn,i);ic.castShadow=!0,n.add(ic);let Ur=new _n,_p=new Float32Array([st,ln,0,st,ie,0,st,Q,Ne,st,ln,Ne]);Ur.setAttribute("position",new vn(_p,3)),Ur.setIndex([0,2,1,0,3,2]),Ur.computeVertexNormals();let rc=new Me(Ur,i);rc.castShadow=!0,n.add(rc),n.add(be);let Br=new Me(new Wt(st,Ne),e.interiorFloor);Br.rotation.x=-Math.PI/2,Br.position.set(st/2,.01,Ne/2),Br.receiveShadow=!0,Br.name="interiorFloor",n.add(Br);let pi=.9,un=2.1,Ii=.15,Ts=new Me(new Wt(Ne,ln-un),e.interiorWall);Ts.rotation.y=Math.PI/2,Ts.position.set(xn,un+(ln-un)/2,Ne/2),n.add(Ts);let Hr=new Me(new Wt(Ii,un),e.interiorWall);Hr.rotation.y=Math.PI/2,Hr.position.set(xn,un/2,Ii/2),n.add(Hr);let oc=Ne-Ii-pi,Vr=new Me(new Wt(oc,un),e.interiorWall);Vr.rotation.y=Math.PI/2,Vr.position.set(xn,un/2,Ii+pi+oc/2),n.add(Vr);let Cs=new Me(new Wt(Ne,ln-un),e.interiorWall);Cs.rotation.y=Math.PI/2,Cs.position.set(Yt,un+(ln-un)/2,Ne/2),n.add(Cs);let Is=new Me(new Wt(Ii,un),e.interiorWall);Is.rotation.y=Math.PI/2,Is.position.set(Yt,un/2,Ii/2),n.add(Is);let eu=Ne-Ii-pi,sc=new Me(new Wt(eu,un),e.interiorWall);sc.rotation.y=Math.PI/2,sc.position.set(Yt,un/2,Ii+pi+eu/2),n.add(sc);let yr=(xn+Yt)/2,xp=new St({color:1118481,metalness:.3,roughness:.2}),tu=new St({color:1710618,metalness:.5,roughness:.3}),nu=1.22,x=.69,L=1.4,z=new Me(new ct(nu,x,.03),xp);z.position.set(yr,L,Ne-.12),n.add(z);let k=new Me(new ct(nu+.04,x+.04,.02),tu);k.position.set(yr,L,Ne-.14),n.add(k);let F=new Me(new ct(.3,.2,.08),tu);F.position.set(yr,L,Ne-.06),n.add(F);let ue=new St({color:4868690,roughness:.85,metalness:0}),_e=new St({color:5592416,roughness:.9,metalness:0}),le=2,Te=.85,Ie=.4,ze=Ne/2+.3,qe=new Me(new ct(le,Ie,Te),ue);qe.position.set(yr,Ie/2,ze),qe.castShadow=!0,qe.receiveShadow=!0,n.add(qe);let De=new Me(new ct(le,.35,.15),ue);De.position.set(yr,Ie+.175,ze-Te/2+.075),De.castShadow=!0,n.add(De);for(let Ce of[-1,1]){let lt=new Me(new ct(.12,.25,Te),ue);lt.position.set(yr+Ce*(le/2-.06),Ie+.125,ze),lt.castShadow=!0,n.add(lt)}for(let Ce=-1;Ce<=1;Ce++){let lt=new Me(new ct(le/3-.04,.08,Te-.2),_e);lt.position.set(yr+Ce*(le/3),Ie+.04,ze+.05),n.add(lt)}let at=1.83,Ot=.45,Ft=.18,Et=new St({color:6044190,roughness:.8}),dn=new St({color:15789284,roughness:.9}),Re=new St({color:16777215,roughness:.95}),kn=Gn/2,pt=Ne-at/2-.15,Kn=Yt+Gn/2,mi=Ne-at/2-.15;for(let[Ce,lt]of[[kn,pt],[Kn,mi]]){let xt=new Me(new ct(at,Ot-Ft,at),Et);xt.position.set(Ce,(Ot-Ft)/2,lt),xt.castShadow=!0,xt.receiveShadow=!0,n.add(xt);let kt=new Me(new ct(at-.04,Ft,at-.04),dn);kt.position.set(Ce,Ot-Ft/2,lt),kt.castShadow=!0,kt.receiveShadow=!0,n.add(kt);let Qt=new Me(new ct(at,.5,.06),Et);Qt.position.set(Ce,Ot+.25,lt+at/2),Qt.castShadow=!0,n.add(Qt);for(let en of[lt+at/2-.25])for(let _r of[Ce-.3,Ce+.3]){let Ep=new Me(new ct(.4,.08,.25),Re);Ep.position.set(_r,Ot+.04,en),Ep.castShadow=!0,n.add(Ep)}}let Ai=new zt,Zi=st,_t=3.2,fn=0,Jn=new Me(new ct(Zi,.1,_t),e.deckTile);Jn.position.set(Zi/2,-.05,-_t/2),Jn.receiveShadow=!0,Jn.castShadow=!0,Ai.add(Jn);let Zt=4;for(let Ce=0;Ce<Zt;Ce++){let lt=Zi/(Zt+1)*(Ce+1),xt=new Me(new ct(.1,.12,_t),e.concrete);xt.position.set(lt,-.08,-_t/2),Ai.add(xt)}n.add(Ai);let Ri=new zt,Di=new Me(new Wt(60,60),e.grass);Di.rotation.x=-Math.PI/2,Di.position.set(st/2,-.15,Ne/2),Di.receiveShadow=!0,Ri.add(Di),n.add(Ri);for(let Ce of M_){let lt=Ce.bounds.maxX-Ce.bounds.minX,xt=Ce.bounds.maxZ-Ce.bounds.minZ,kt=(Ce.bounds.minX+Ce.bounds.maxX)/2,Qt=(Ce.bounds.minZ+Ce.bounds.maxZ)/2,en=new Me(new Wt(lt,xt),new vs({visible:!1}));en.rotation.x=-Math.PI/2,en.position.set(kt,.02,Qt),en.name=`room_${Ce.name}`,en.userData={room:Ce},n.add(en)}return n.position.set(-st/2,0,-Ne/2),n}var gp=class{renderer;labelRenderer;scene;camera;orbitControls;pointerControls;house;raycaster=new Bl;mouse=new Ue;animationId=0;clock=new Hl;moveForward=!1;moveBackward=!1;moveLeft=!1;moveRight=!1;velocity=new T;direction=new T;moveSpeed=5;roomLabels=[];dimensionLabels=[];dimensionLines=[];labelsVisible=!0;dimensionsVisible=!1;doorIsOpen=!0;doorTarget=1;doorProgress=1;windowsOpen=!1;windowTarget=0;windowProgress=0;mode="orbit";callbacks;container;constructor(e,t,i={}){this.container=e,this.callbacks=i,this.initRenderer(t),this.initScene(),this.initCamera(),this.initControls(),this.initLighting(),this.initLabelRenderer(),this.buildHouse(),this.createLabels(),this.createDimensionMarkers(),this.bindEvents(),this.animate()}initRenderer(e){this.renderer=new op({canvas:e,antialias:!0,alpha:!1}),this.renderer.setSize(this.container.clientWidth,this.container.clientHeight),this.renderer.setPixelRatio(Math.min(window.devicePixelRatio,2)),this.renderer.shadowMap.enabled=!0,this.renderer.shadowMap.type=dh,this.renderer.toneMapping=zl,this.renderer.toneMappingExposure=1.2}initScene(){this.scene=new Al,this.scene.background=new Qe(13166335),this.scene.fog=new Il(13166335,30,80)}initCamera(){let e=this.container.clientWidth/this.container.clientHeight;this.camera=new On(55,e,.1,200),this.camera.position.set(12,8,-14),this.camera.lookAt(0,1,0)}initControls(){this.orbitControls=new lp(this.camera,this.renderer.domElement),this.orbitControls.target.set(0,1,0),this.orbitControls.enableDamping=!0,this.orbitControls.dampingFactor=.08,this.orbitControls.maxPolarAngle=Math.PI/2.05,this.orbitControls.minDistance=3,this.orbitControls.maxDistance=40,this.orbitControls.update(),this.pointerControls=new up(this.camera,this.renderer.domElement),this.pointerControls.addEventListener("lock",()=>{this.callbacks.onModeChange?.("walkthrough")}),this.pointerControls.addEventListener("unlock",()=>{})}initLighting(){let e=new Fl(8900331,9139029,.6);this.scene.add(e);let t=new Ul(16777215,.3);this.scene.add(t);let i=new ja(16774624,1.5);i.position.set(15,20,-10),i.castShadow=!0,i.shadow.mapSize.width=2048,i.shadow.mapSize.height=2048,i.shadow.camera.left=-20,i.shadow.camera.right=20,i.shadow.camera.top=20,i.shadow.camera.bottom=-20,i.shadow.camera.near=.5,i.shadow.camera.far=60,i.shadow.bias=-.001,this.scene.add(i);let r=new ja(11193599,.4);r.position.set(-10,8,10),this.scene.add(r)}initLabelRenderer(){this.labelRenderer=new dp,this.labelRenderer.setSize(this.container.clientWidth,this.container.clientHeight),this.labelRenderer.domElement.style.position="absolute",this.labelRenderer.domElement.style.top="0",this.labelRenderer.domElement.style.left="0",this.labelRenderer.domElement.style.pointerEvents="none",this.container.appendChild(this.labelRenderer.domElement)}buildHouse(){this.house=yw(),this.scene.add(this.house)}createLabels(){for(let e of M_){let t=document.createElement("div");t.className="room-label",t.textContent=e.name,t.style.cssText=`
        background: rgba(0,0,0,0.7);
        color: white;
        padding: 4px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-family: system-ui, sans-serif;
        white-space: nowrap;
        border-left: 3px solid #${e.color.toString(16).padStart(6,"0")};
      `;let i=new Jl(t);i.position.copy(e.labelPos),i.position.x-=st/2,i.position.z-=Ne/2,this.roomLabels.push(i),this.scene.add(i)}}createDimensionMarkers(){let e=new ys({color:16729156,linewidth:2});for(let t of vw){let i=t.start.clone().add(new T(-st/2,0,-Ne/2)),r=t.end.clone().add(new T(-st/2,0,-Ne/2)),o=[i,r],s=new _n().setFromPoints(o),a=t.color?new ys({color:t.color}):e,c=new Nl(s,a);c.visible=this.dimensionsVisible,this.dimensionLines.push(c),this.scene.add(c);let l=i.clone().add(r).multiplyScalar(.5).add(t.offset),u=document.createElement("div");u.className="dim-label",u.textContent=t.label;let d=t.color||"rgba(255,60,60,0.85)";u.style.cssText=`
        background: ${d};
        color: white;
        padding: 2px 8px;
        border-radius: 3px;
        font-size: 11px;
        font-weight: bold;
        font-family: monospace;
      `;let f=new Jl(u);f.position.copy(l),f.visible=this.dimensionsVisible,this.dimensionLabels.push(f),this.scene.add(f)}}bindEvents(){window.addEventListener("resize",this.onResize),this.renderer.domElement.addEventListener("click",this.onClick),document.addEventListener("keydown",this.onKeyDown),document.addEventListener("keyup",this.onKeyUp)}onResize=()=>{let e=this.container.clientWidth,t=this.container.clientHeight;this.camera.aspect=e/t,this.camera.updateProjectionMatrix(),this.renderer.setSize(e,t),this.labelRenderer.setSize(e,t)};onClick=e=>{let t=this.renderer.domElement.getBoundingClientRect();this.mouse.x=(e.clientX-t.left)/t.width*2-1,this.mouse.y=-((e.clientY-t.top)/t.height)*2+1,this.raycaster.setFromCamera(this.mouse,this.camera);let i=this.raycaster.intersectObjects(this.house.children,!0),r=null;for(let o of i)if(o.object.userData?.room){r=o.object.userData.room;break}this.callbacks.onRoomClick?.(r)};onKeyDown=e=>{if(this.mode==="walkthrough")switch(e.code){case"KeyW":case"ArrowUp":this.moveForward=!0;break;case"KeyS":case"ArrowDown":this.moveBackward=!0;break;case"KeyA":case"ArrowLeft":this.moveLeft=!0;break;case"KeyD":case"ArrowRight":this.moveRight=!0;break}};onKeyUp=e=>{switch(e.code){case"KeyW":case"ArrowUp":this.moveForward=!1;break;case"KeyS":case"ArrowDown":this.moveBackward=!1;break;case"KeyA":case"ArrowLeft":this.moveLeft=!1;break;case"KeyD":case"ArrowRight":this.moveRight=!1;break}};updateWalkthrough(e){this.mode==="walkthrough"&&(this.velocity.x-=this.velocity.x*10*e,this.velocity.z-=this.velocity.z*10*e,this.direction.z=Number(this.moveForward)-Number(this.moveBackward),this.direction.x=Number(this.moveRight)-Number(this.moveLeft),this.direction.normalize(),(this.moveForward||this.moveBackward)&&(this.velocity.z-=this.direction.z*this.moveSpeed*e),(this.moveLeft||this.moveRight)&&(this.velocity.x-=this.direction.x*this.moveSpeed*e),this.pointerControls.moveRight(-this.velocity.x*e),this.pointerControls.moveForward(-this.velocity.z*e),this.camera.position.y=1.6)}animate=()=>{this.animationId=requestAnimationFrame(this.animate);let e=this.clock.getDelta();this.mode==="orbit"?this.orbitControls.update():this.updateWalkthrough(e),this.updateDoorAnimation(e),this.updateWindowAnimation(e),this.renderer.render(this.scene,this.camera),this.labelRenderer.render(this.scene,this.camera)};updateDoorAnimation(e){if(!this.house)return;let t=this.doorTarget-this.doorProgress;if(Math.abs(t)<.001)return;let i=2;this.doorProgress+=Math.sign(t)*i*e,this.doorProgress=Math.max(0,Math.min(1,this.doorProgress));try{let r=this.house.userData.doorLeftLeaf,o=this.house.userData.doorRightLeaf,s=this.house.userData.doorOpenAngle||Math.PI*.4;if(r&&o){let a=s*this.doorProgress;r.rotation.y=a,o.rotation.y=-a}}catch{}}updateWindowAnimation(e){if(!this.house)return;let t=this.windowTarget-this.windowProgress;if(Math.abs(t)<.001)return;let i=2;this.windowProgress+=Math.sign(t)*i*e,this.windowProgress=Math.max(0,Math.min(1,this.windowProgress));try{let r=this.house.userData.windowLeaves,o=this.house.userData.windowOpenAngle||Math.PI*.35;if(r)for(let{leaf:s,sign:a}of r)s.rotation.y=a*o*this.windowProgress}catch{}}setMode(e){this.mode=e,e==="orbit"?(this.pointerControls.unlock(),this.orbitControls.enabled=!0,this.camera.position.set(12,8,-14),this.orbitControls.target.set(0,1,0),this.orbitControls.update()):(this.orbitControls.enabled=!1,this.camera.position.set(-st/2+2.5,1.6,0),this.camera.lookAt(-st/2+6,1.6,0),this.pointerControls.lock()),this.callbacks.onModeChange?.(e)}getMode(){return this.mode}toggleLabels(){this.labelsVisible=!this.labelsVisible;for(let e of this.roomLabels)e.visible=this.labelsVisible;return this.labelsVisible}toggleDoor(){return this.doorIsOpen=!this.doorIsOpen,this.doorTarget=this.doorIsOpen?1:0,this.doorIsOpen}getDoorOpen(){return this.doorIsOpen}toggleWindows(){return this.windowsOpen=!this.windowsOpen,this.windowTarget=this.windowsOpen?1:0,this.windowsOpen}getWindowsOpen(){return this.windowsOpen}toggleDimensions(){this.dimensionsVisible=!this.dimensionsVisible;for(let e of this.dimensionLabels)e.visible=this.dimensionsVisible;for(let e of this.dimensionLines)e.visible=this.dimensionsVisible;return this.dimensionsVisible}dispose(){cancelAnimationFrame(this.animationId),window.removeEventListener("resize",this.onResize),this.renderer.domElement.removeEventListener("click",this.onClick),document.removeEventListener("keydown",this.onKeyDown),document.removeEventListener("keyup",this.onKeyUp),this.orbitControls.dispose(),this.pointerControls.dispose(),this.renderer.dispose(),this.labelRenderer.domElement.remove()}};var B2=["sceneContainer"],H2=["renderCanvas"];function V2(n,e){n&1&&(Ei(),ne(0,"svg",8),ii(1,"path",36)(2,"circle",37),se(),Mi(),ne(3,"span",12),ve(4,"Walk-through"),se())}function z2(n,e){n&1&&(Ei(),ne(0,"svg",8),ii(1,"circle",38)(2,"path",39)(3,"path",40),se(),Mi(),ne(4,"span",12),ve(5,"Orbit View"),se())}function G2(n,e){n&1&&(Ei(),ne(0,"svg",8),ii(1,"polygon",41)(2,"path",42)(3,"path",43),se(),Mi(),ne(4,"span",12),ve(5,"Music"),se())}function W2(n,e){n&1&&(Ei(),ne(0,"svg",8),ii(1,"polygon",41)(2,"line",44)(3,"line",45),se(),Mi(),ne(4,"span",12),ve(5,"Muted"),se())}function j2(n,e){if(n&1){let t=$c();ne(0,"div",32)(1,"div",46)(2,"span"),ve(3,"Materials List"),se(),ne(4,"button",47),qn("click",function(){$s(t);let r=ca();return qs(r.toggleMaterials())}),ve(5,"\xD7"),se()(),ne(6,"div",48)(7,"p",49),ve(8,"Structural"),se(),ne(9,"ul")(10,"li"),ve(11,"2x 40ft High-Cube Shipping Containers"),se(),ne(12,"li"),ve(13,"Concrete foundation slab (9.5m x 4.1m x 0.15m)"),se(),ne(14,"li"),ve(15,"Steel support columns x4"),se()(),ne(16,"p",49),ve(17,"Roofing"),se(),ne(18,"ul")(19,"li"),ve(20,"Metal roofing panels (dark finish) ~45 m\xB2"),se(),ne(21,"li"),ve(22,"Timber soffit boards (underside) ~45 m\xB2"),se(),ne(23,"li"),ve(24,"Steel fascia boards x4 sides"),se()(),ne(25,"p",49),ve(26,"Exterior Walls"),se(),ne(27,"ul")(28,"li"),ve(29,"Brick cladding \u2014 front & back walls ~48 m\xB2"),se(),ne(30,"li"),ve(31,"Brick cladding \u2014 side walls ~19 m\xB2"),se(),ne(32,"li"),ve(33,"Mortar & brick ties"),se()(),ne(34,"p",49),ve(35,"Windows"),se(),ne(36,"ul")(37,"li"),ve(38,"2x Front casement windows (1.86m x 1.50m)"),se(),ne(39,"li"),ve(40,"2x Side casement windows (1.86m x 1.50m)"),se(),ne(41,"li"),ve(42,"2x Back slim windows (0.30m x 1.80m)"),se(),ne(43,"li"),ve(44,"Steel window frames x6"),se(),ne(45,"li"),ve(46,"Double-glazed glass panes x6"),se()(),ne(47,"p",49),ve(48,"Door"),se(),ne(49,"ul")(50,"li"),ve(51,"1x French door \u2014 4 panel (2 fixed + 2 swing)"),se(),ne(52,"li"),ve(53,"Steel door frame with transom"),se(),ne(54,"li"),ve(55,"Tempered glass panels x4"),se(),ne(56,"li"),ve(57,"Door hinges & hardware"),se()(),ne(58,"p",49),ve(59,"Interior"),se(),ne(60,"ul")(61,"li"),ve(62,"Plasterboard partition walls x2"),se(),ne(63,"li"),ve(64,"Interior doors (0.9m x 2.1m) x2"),se(),ne(65,"li"),ve(66,"Hardwood flooring ~33 m\xB2"),se(),ne(67,"li"),ve(68,"Interior wall paint / finish"),se()(),ne(69,"p",49),ve(70,"Veranda"),se(),ne(71,"ul")(72,"li"),ve(73,"Deck tiles / pavers ~29 m\xB2"),se(),ne(74,"li"),ve(75,"Concrete support beams x4"),se()(),ne(76,"p",49),ve(77,"Furniture"),se(),ne(78,"ul")(79,"li"),ve(80,"2x King-size beds with headboards"),se(),ne(81,"li"),ve(82,"1x 3-seater sofa with armrests"),se(),ne(83,"li"),ve(84,'1x 55" Wall-mounted TV with bracket'),se()()()()}}function $2(n,e){if(n&1){let t=$c();ne(0,"div",33)(1,"div",46)(2,"span"),ve(3,"Controls"),se(),ne(4,"button",47),qn("click",function(){$s(t);let r=ca();return qs(r.toggleHelp())}),ve(5,"\xD7"),se()(),ne(6,"div",50)(7,"p")(8,"strong"),ve(9,"Orbit Mode:"),se()(),ne(10,"ul")(11,"li"),ve(12,"Left-click + drag: Rotate"),se(),ne(13,"li"),ve(14,"Right-click + drag: Pan"),se(),ne(15,"li"),ve(16,"Scroll: Zoom in/out"),se(),ne(17,"li"),ve(18,"Click on floor: Select room"),se()(),ne(19,"p")(20,"strong"),ve(21,"Walk-through Mode:"),se()(),ne(22,"ul")(23,"li"),ve(24,"WASD / Arrow keys: Move"),se(),ne(25,"li"),ve(26,"Mouse: Look around"),se(),ne(27,"li"),ve(28,"ESC: Release cursor"),se(),ne(29,"li"),ve(30,"Click to re-lock cursor"),se()()()()}}function q2(n,e){if(n&1){let t=$c();ne(0,"div",34)(1,"div",51)(2,"span",52),ve(3),se(),ne(4,"button",47),qn("click",function(){$s(t);let r=ca();return qs(r.closeRoomPanel())}),ve(5,"\xD7"),se()(),ne(6,"div",53)(7,"p",54),ve(8),se(),ne(9,"div",55)(10,"span",56),ve(11,"Area"),se(),ne(12,"span",57),ve(13),se()()()()}if(n&2){let t=e,i=ca();Kt(),Od("border-left-color",i.getRoomColor(t)),Kt(2),oo(t.name),Kt(5),oo(t.description),Kt(5),oo(t.area)}}function X2(n,e){n&1&&(ne(0,"div",35)(1,"div",58),ve(2,"+"),se(),ne(3,"div",59),ve(4,"WASD to move \xB7 Mouse to look \xB7 ESC to exit"),se()())}var vp=class n{containerRef;canvasRef;sceneManager;mode=Nn("orbit");labelsOn=Nn(!0);dimensionsOn=Nn(!1);doorOpen=Nn(!0);windowsOpen=Nn(!1);selectedRoom=Nn(null);showHelp=Nn(!1);showMaterials=Nn(!1);muted=Nn(!1);audio;ngAfterViewInit(){this.sceneManager=new gp(this.containerRef.nativeElement,this.canvasRef.nativeElement,{onRoomClick:t=>this.selectedRoom.set(t),onModeChange:t=>this.mode.set(t)}),this.audio=new Audio("music/nyasango-brian-sigu.mp3"),this.audio.loop=!0,this.audio.volume=.4;let e=()=>{this.audio.play().catch(()=>{}),document.removeEventListener("click",e),document.removeEventListener("keydown",e)};document.addEventListener("click",e),document.addEventListener("keydown",e)}ngOnDestroy(){this.audio?.pause(),this.sceneManager?.dispose()}toggleMode(){let e=this.mode()==="orbit"?"walkthrough":"orbit";this.sceneManager.setMode(e),this.mode.set(e)}toggleLabels(){let e=this.sceneManager.toggleLabels();this.labelsOn.set(e)}toggleDimensions(){let e=this.sceneManager.toggleDimensions();this.dimensionsOn.set(e)}toggleDoor(){let e=this.sceneManager.toggleDoor();this.doorOpen.set(e)}toggleWindows(){let e=this.sceneManager.toggleWindows();this.windowsOpen.set(e)}closeRoomPanel(){this.selectedRoom.set(null)}toggleHelp(){this.showHelp.set(!this.showHelp())}toggleMaterials(){this.showMaterials.set(!this.showMaterials())}toggleMute(){let e=!this.muted();this.muted.set(e),this.audio.muted=e}getRoomColor(e){return"#"+e.color.toString(16).padStart(6,"0")}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ns({type:n,selectors:[["app-scene"]],viewQuery:function(t,i){if(t&1&&Nd(B2,7)(H2,7),t&2){let r;Pd(r=Ld())&&(i.containerRef=r.first),Pd(r=Ld())&&(i.canvasRef=r.first)}},decls:57,vars:24,consts:[["sceneContainer",""],["renderCanvas",""],[1,"scene-container"],[1,"toolbar"],[1,"toolbar-title"],[1,"toolbar-buttons"],[1,"tb-btn",3,"click","title"],["title","Toggle Room Labels",1,"tb-btn",3,"click"],["width","18","height","18","viewBox","0 0 24 24","fill","none","stroke","currentColor","stroke-width","2"],["d","M4 7V4h16v3"],["d","M9 20h6"],["d","M12 4v16"],[1,"btn-text"],["title","Toggle Dimensions",1,"tb-btn",3,"click"],["d","M21 13V8a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v5"],["d","M3 17h18"],["d","M3 15v4"],["d","M21 15v4"],["title","Open/Close Door",1,"tb-btn",3,"click"],["x","3","y","3","width","14","height","18","rx","1"],["d","M17 3v18"],["cx","15","cy","12","r","1"],["title","Open/Close Bedroom Windows",1,"tb-btn",3,"click"],["x","3","y","3","width","18","height","18","rx","1"],["d","M12 3v18"],["d","M3 12h18"],["title","Materials List",1,"tb-btn",3,"click"],["d","M9 5H7a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2h-2"],["x","9","y","3","width","6","height","4","rx","1"],["d","M9 12h6"],["d","M9 16h6"],["title","Help",1,"tb-btn","help-btn",3,"click"],[1,"materials-panel"],[1,"help-panel"],[1,"room-panel"],[1,"mode-indicator"],["d","M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"],["cx","12","cy","7","r","4"],["cx","12","cy","12","r","10"],["d","M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"],["d","M2 12h20"],["points","11 5 6 9 2 9 2 15 6 15 11 19 11 5"],["d","M15.54 8.46a5 5 0 0 1 0 7.07"],["d","M19.07 4.93a10 10 0 0 1 0 14.14"],["x1","23","y1","9","x2","17","y2","15"],["x1","17","y1","9","x2","23","y2","15"],[1,"help-header"],[1,"close-btn",3,"click"],[1,"materials-content"],[1,"mat-section"],[1,"help-content"],[1,"room-header"],[1,"room-name"],[1,"room-body"],[1,"room-desc"],[1,"room-stat"],[1,"stat-label"],[1,"stat-value"],[1,"crosshair"],[1,"mode-hint"]],template:function(t,i){if(t&1&&(ne(0,"div",2,0),ii(2,"canvas",null,1),ne(4,"div",3)(5,"div",4),ve(6,"Simba design"),se(),ne(7,"div",5)(8,"button",6),qn("click",function(){return i.toggleMode()}),io(9,V2,5,0)(10,z2,6,0),se(),ne(11,"button",7),qn("click",function(){return i.toggleLabels()}),Ei(),ne(12,"svg",8),ii(13,"path",9)(14,"path",10)(15,"path",11),se(),Mi(),ne(16,"span",12),ve(17,"Labels"),se()(),ne(18,"button",13),qn("click",function(){return i.toggleDimensions()}),Ei(),ne(19,"svg",8),ii(20,"path",14)(21,"path",15)(22,"path",16)(23,"path",17),se(),Mi(),ne(24,"span",12),ve(25,"Dimensions"),se()(),ne(26,"button",18),qn("click",function(){return i.toggleDoor()}),Ei(),ne(27,"svg",8),ii(28,"rect",19)(29,"path",20)(30,"circle",21),se(),Mi(),ne(31,"span",12),ve(32),se()(),ne(33,"button",22),qn("click",function(){return i.toggleWindows()}),Ei(),ne(34,"svg",8),ii(35,"rect",23)(36,"path",24)(37,"path",25),se(),Mi(),ne(38,"span",12),ve(39),se()(),ne(40,"button",26),qn("click",function(){return i.toggleMaterials()}),Ei(),ne(41,"svg",8),ii(42,"path",27)(43,"rect",28)(44,"path",29)(45,"path",30),se(),Mi(),ne(46,"span",12),ve(47,"Material"),se()(),ne(48,"button",6),qn("click",function(){return i.toggleMute()}),io(49,G2,6,0)(50,W2,6,0),se(),ne(51,"button",31),qn("click",function(){return i.toggleHelp()}),ve(52," ? "),se()()(),io(53,j2,85,0,"div",32),io(54,$2,31,0,"div",33),io(55,q2,14,5,"div",34),io(56,X2,5,0,"div",35),se()),t&2){let r;Kt(8),or("active",i.mode()==="walkthrough"),qc("title",i.mode()==="orbit"?"Switch to Walk-through":"Switch to Orbit View"),Kt(),ro(i.mode()==="orbit"?9:10),Kt(2),or("active",i.labelsOn()),Kt(7),or("active",i.dimensionsOn()),Kt(8),or("active",i.doorOpen()),Kt(6),oo(i.doorOpen()?"Close Door":"Open Door"),Kt(),or("active",i.windowsOpen()),Kt(6),oo(i.windowsOpen()?"Close Windows":"Open Windows"),Kt(),or("active",i.showMaterials()),Kt(8),or("active",!i.muted()),qc("title",i.muted()?"Unmute Music":"Mute Music"),Kt(),ro(i.muted()?50:49),Kt(4),ro(i.showMaterials()?53:-1),Kt(),ro(i.showHelp()?54:-1),Kt(),ro((r=i.selectedRoom())?55:-1,r),Kt(),ro(i.mode()==="walkthrough"?56:-1)}},styles:["[_nghost-%COMP%]{display:block;width:100%;height:100%}.scene-container[_ngcontent-%COMP%]{position:relative;width:100%;height:100%;overflow:hidden;background:#1a1a2e}.scene-container[_ngcontent-%COMP%]   canvas[_ngcontent-%COMP%]{display:block;width:100%;height:100%}.toolbar[_ngcontent-%COMP%]{position:absolute;top:12px;left:12px;right:12px;display:flex;align-items:center;justify-content:space-between;padding:8px 14px;background:#0f0f1ecc;-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-radius:10px;border:1px solid rgba(255,255,255,.08);z-index:10;pointer-events:auto}.toolbar-title[_ngcontent-%COMP%]{color:#fff;font-size:14px;font-weight:600;font-family:system-ui,sans-serif;letter-spacing:.02em}.toolbar-buttons[_ngcontent-%COMP%]{display:flex;gap:6px;align-items:center}.tb-btn[_ngcontent-%COMP%]{display:flex;align-items:center;gap:5px;padding:6px 12px;border:1px solid rgba(255,255,255,.12);border-radius:6px;background:#ffffff0f;color:#ffffffbf;font-size:12px;font-family:system-ui,sans-serif;cursor:pointer;transition:all .2s ease;white-space:nowrap}.tb-btn[_ngcontent-%COMP%]:hover{background:#ffffff1f;color:#fff}.tb-btn.active[_ngcontent-%COMP%]{background:#2196f340;border-color:#2196f380;color:#64b5f6}.tb-btn[_ngcontent-%COMP%]   svg[_ngcontent-%COMP%]{flex-shrink:0}.help-btn[_ngcontent-%COMP%]{width:30px;height:30px;padding:0;justify-content:center;font-weight:700;font-size:14px;border-radius:50%}.materials-panel[_ngcontent-%COMP%]{position:absolute;top:65px;right:12px;width:300px;max-height:calc(100% - 85px);overflow-y:auto;background:#0f0f1ee6;-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-radius:10px;border:1px solid rgba(255,255,255,.08);z-index:10;color:#ffffffd9;font-family:system-ui,sans-serif;font-size:12px}.materials-content[_ngcontent-%COMP%]{padding:10px 14px;line-height:1.6}.materials-content[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0 0 6px;padding-left:16px}.materials-content[_ngcontent-%COMP%]   li[_ngcontent-%COMP%]{color:#ffffffa6}.mat-section[_ngcontent-%COMP%]{margin:8px 0 2px;font-weight:600;font-size:12px;color:#64b5f6}.mat-section[_ngcontent-%COMP%]:first-child{margin-top:0}@media(max-width:600px){.toolbar[_ngcontent-%COMP%]{top:8px;left:8px;right:8px;padding:6px 10px;border-radius:8px}.toolbar-title[_ngcontent-%COMP%]{font-size:13px}.toolbar-buttons[_ngcontent-%COMP%]{gap:4px}.tb-btn[_ngcontent-%COMP%]{padding:6px 8px;gap:0}.tb-btn[_ngcontent-%COMP%]   .btn-text[_ngcontent-%COMP%]{display:none}.help-btn[_ngcontent-%COMP%]{width:28px;height:28px;font-size:13px}.help-panel[_ngcontent-%COMP%], .materials-panel[_ngcontent-%COMP%]{top:55px;right:8px;left:8px;width:auto;max-height:calc(100% - 70px)}.room-panel[_ngcontent-%COMP%]{left:8px;right:8px;bottom:8px;width:auto}.mode-hint[_ngcontent-%COMP%]{font-size:11px;padding:5px 12px;bottom:12px}}.help-panel[_ngcontent-%COMP%]{position:absolute;top:65px;right:12px;width:260px;background:#0f0f1ee6;-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-radius:10px;border:1px solid rgba(255,255,255,.08);z-index:10;color:#ffffffd9;font-family:system-ui,sans-serif;font-size:12px}.help-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.08);font-weight:600;font-size:13px}.help-content[_ngcontent-%COMP%]{padding:10px 14px;line-height:1.6}.help-content[_ngcontent-%COMP%]   p[_ngcontent-%COMP%]{margin:6px 0 2px}.help-content[_ngcontent-%COMP%]   ul[_ngcontent-%COMP%]{margin:0;padding-left:16px}.help-content[_ngcontent-%COMP%]   li[_ngcontent-%COMP%]{color:#ffffffa6}.room-panel[_ngcontent-%COMP%]{position:absolute;bottom:16px;left:16px;width:280px;background:#0f0f1ee6;-webkit-backdrop-filter:blur(12px);backdrop-filter:blur(12px);border-radius:10px;border:1px solid rgba(255,255,255,.08);z-index:10;color:#fff;font-family:system-ui,sans-serif;animation:_ngcontent-%COMP%_slideUp .25s ease}@keyframes _ngcontent-%COMP%_slideUp{0%{opacity:0;transform:translateY(12px)}to{opacity:1;transform:translateY(0)}}.room-header[_ngcontent-%COMP%]{display:flex;justify-content:space-between;align-items:center;padding:12px 14px;border-bottom:1px solid rgba(255,255,255,.08);border-left:3px solid #4caf50;border-radius:10px 10px 0 0}.room-name[_ngcontent-%COMP%]{font-weight:600;font-size:15px}.room-body[_ngcontent-%COMP%]{padding:12px 14px}.room-desc[_ngcontent-%COMP%]{margin:0 0 10px;color:#ffffffa6;font-size:12px;line-height:1.5}.room-stat[_ngcontent-%COMP%]{display:flex;justify-content:space-between;padding:6px 10px;background:#ffffff0d;border-radius:5px;font-size:12px}.stat-label[_ngcontent-%COMP%]{color:#ffffff80}.stat-value[_ngcontent-%COMP%]{font-weight:600;color:#64b5f6}.close-btn[_ngcontent-%COMP%]{background:none;border:none;color:#ffffff80;font-size:20px;cursor:pointer;padding:0 4px;line-height:1}.close-btn[_ngcontent-%COMP%]:hover{color:#fff}.mode-indicator[_ngcontent-%COMP%]{position:absolute;inset:0;pointer-events:none;z-index:5}.crosshair[_ngcontent-%COMP%]{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#fff9;font-size:24px;font-weight:300;line-height:1}.mode-hint[_ngcontent-%COMP%]{position:absolute;bottom:20px;left:50%;transform:translate(-50%);background:#0009;color:#ffffffb3;padding:6px 16px;border-radius:20px;font-size:12px;font-family:system-ui,sans-serif;white-space:nowrap}"]})};var yp=class n{static \u0275fac=function(t){return new(t||n)};static \u0275cmp=ns({type:n,selectors:[["app-root"]],decls:1,vars:0,template:function(t,i){t&1&&aa(0,"app-scene")},dependencies:[vp],styles:["[_nghost-%COMP%]{display:block;width:100%;height:100dvh;margin:0;padding:0;overflow:hidden}"]})};dv(yp,bb).catch(n=>console.error(n));
