
// ============================================================================
// FF_Rotation.cpp -- all-contained factor pipeline: F6 WRLD US regressors +
// Every persistent artifact is SQLite (no CSV anywhere in the pipeline).
//
//   China ETFs (dumb_china_factor.py) wired into the FF factor builder.
//              (Yahoo adjusted-close history for every supported region)
//              + US/EU ETF universe from
//              cleyfe/os-bloom config.yaml + China ETF universe embedded
//              below (extracted from dumb_china_factor.py) -> SQLite
//   F6 WRLD US rows -> EDGAR-transformed regressors; embedded/os-bloom ETFs -> Y
//   build   -> canonical FF6 (2x3 VW sorts w/ daily drift, June rebalance,
//              12-2 UMD) from SQLite panels -> factors table
//   rotate  -> factor-momentum + group xsec-mean-beta tables per horizon
//              {7,14,28,62,128,256}, .shift(1), canonical OLS -> SQL tables
//              factor_momentum / xsec_mean_beta + aligned console report
//
// Build:  g++ -O3 -march=native -std=c++17 FF_Rotation.cpp -o ff_rotation -lcurl -lsqlite3
// ============================================================================
#include <bits/stdc++.h>
#include <sqlite3.h>
using namespace std;

// ============================== utils ======================================
static vector<string> split_csv(const string& s){vector<string> o;string c;
    for(char x:s){if(x==','){o.push_back(c);c.clear();}else c+=x;}o.push_back(c);return o;}
static double to_d(const string& s){if(s.empty())return 0;char*e=nullptr;double v=strtod(s.c_str(),&e);
    return e==s.c_str()?NAN:v;}
static string trim(const string& s){size_t a=s.find_first_not_of(" \t\r\n"),b=s.find_last_not_of(" \t\r\n");
    return a==string::npos?"":s.substr(a,b-a+1);}
static string ymd_norm(string d){d=trim(d);if(d.size()==8&&d.find('-')==string::npos)
    return d.substr(0,4)+"-"+d.substr(4,2)+"-"+d.substr(6,2);return d;}
static string jstr(const string& js,const string& key){size_t p=js.find("\""+key+"\"");
    if(p==string::npos){p=js.find(key);if(p==string::npos)return "";}
    p=js.find(':',p);if(p==string::npos)return "";++p;
    while(p<js.size()&&isspace((unsigned char)js[p]))++p;
    if(js[p]=='"'){size_t q=js.find('"',p+1);return js.substr(p+1,q-p-1);}
    size_t q=p;while(q<js.size()&&(isdigit((unsigned char)js[q])||string("-._deE+").find(js[q])!=string::npos))++q;
    return js.substr(p,q-p);}
static vector<double> jarr(const string& js,const string& key){size_t p=js.find("\""+key+"\"");
    if(p==string::npos)return {};p=js.find('[',p);if(p==string::npos)return {};
    int d=0;vector<double> v;string num;
    for(size_t q=p;q<js.size();++q){char c=js[q];
        if(c=='[')++d;else if(c==']'){if(--d==0)break;}
        else if(d==1&&(isdigit((unsigned char)c)||string("-._eE+").find(c)!=string::npos))num+=c;
        else if(!num.empty()){v.push_back(to_d(num));num.clear();}}
    if(!num.empty())v.push_back(to_d(num));return v;}

// ============================== sqlite RAII =================================
struct Db {
    sqlite3* h=nullptr;
    void open(const string& path){ if(sqlite3_open(path.c_str(),&h)!=SQLITE_OK)
        {fprintf(stderr,"db open: %s\n",sqlite3_errmsg(h));exit(1);}
        exec("PRAGMA journal_mode=WAL"); exec("PRAGMA synchronous=OFF");
        exec("PRAGMA mmap_size=268435456"); }
    void exec(const string& sql){ char* e=nullptr;
        if(sqlite3_exec(h,sql.c_str(),nullptr,nullptr,&e)!=SQLITE_OK)
            {fprintf(stderr,"SQL: %s | %s\n",e,sql.c_str());sqlite3_free(e);exit(1);} }
    struct Stmt { sqlite3_stmt* s=nullptr; ~Stmt(){if(s)sqlite3_finalize(s);}
        void prep(sqlite3* h,const string& q){ if(sqlite3_prepare_v2(h,q.c_str(),-1,&s,nullptr)!=SQLITE_OK)
            {fprintf(stderr,"prep: %s\n",sqlite3_errmsg(h));exit(1);} }
        void bind(int i,const string& v){sqlite3_bind_text(s,i,v.c_str(),-1,SQLITE_TRANSIENT);}
        void bind(int i,double v){sqlite3_bind_double(s,i,v);}
        bool step(){int rc=sqlite3_step(s);return rc==SQLITE_ROW;}
        double colD(int i)const{return sqlite3_column_double(s,i);}
        bool isNull(int i)const{return sqlite3_column_type(s,i)==SQLITE_NULL;}
        string colS(int i)const{return (const char*)sqlite3_column_text(s,i);}
        void reset(){sqlite3_reset(s);sqlite3_clear_bindings(s);}
    };
    void tx_begin(){exec("BEGIN");} void tx_commit(){exec("COMMIT");}
    long count(const string& table){ Stmt st; st.prep(h,("SELECT COUNT(*) FROM "+table).c_str());
        long n=0; if(st.step()) n=sqlite3_column_int64(st.s,0); return n; }
};

// ============================== http ========================================
#ifndef NO_CURL
#include <curl/curl.h>
static size_t wr(char* p,size_t s,size_t n,void* u){((string*)u)->append(p,s*n);return s*n;}
static string http_get_timeout(const string& url,const string& ua,long timeout,long connect){ string b; CURL* c=curl_easy_init();
    curl_easy_setopt(c,CURLOPT_URL,url.c_str()); curl_easy_setopt(c,CURLOPT_WRITEFUNCTION,wr);
    curl_easy_setopt(c,CURLOPT_WRITEDATA,&b); curl_easy_setopt(c,CURLOPT_USERAGENT,ua.c_str());
    curl_easy_setopt(c,CURLOPT_TIMEOUT,timeout); curl_easy_setopt(c,CURLOPT_CONNECTTIMEOUT,connect);
    curl_easy_setopt(c,CURLOPT_NOSIGNAL,1L);
    curl_easy_setopt(c,CURLOPT_LOW_SPEED_LIMIT,128L); curl_easy_setopt(c,CURLOPT_LOW_SPEED_TIME,30L);
    curl_easy_setopt(c,CURLOPT_FOLLOWLOCATION,1L);
    CURLcode rc=curl_easy_perform(c); curl_easy_cleanup(c);
    if(rc!=CURLE_OK){fprintf(stderr,"GET failed %s\n",url.c_str());return {};}
    return b; }
static string http_get(const string& url,const string& ua){ return http_get_timeout(url,ua,90L,20L); }
static string http_get_sec(const string& url,const string& ua){ return http_get_timeout(url,ua,12L,5L); }
static vector<string> http_get_sec_many(const vector<string>& urls,const string& ua, vector<long>& codes){
    vector<string> out(urls.size()); codes.assign(urls.size(),0); CURLM* m=curl_multi_init(); vector<CURL*> hs;
    for(size_t i=0;i<urls.size();++i){ CURL* c=curl_easy_init(); hs.push_back(c);
        curl_easy_setopt(c,CURLOPT_URL,urls[i].c_str()); curl_easy_setopt(c,CURLOPT_WRITEFUNCTION,wr);
        curl_easy_setopt(c,CURLOPT_WRITEDATA,&out[i]); curl_easy_setopt(c,CURLOPT_USERAGENT,ua.c_str());
        curl_easy_setopt(c,CURLOPT_TIMEOUT,12L); curl_easy_setopt(c,CURLOPT_CONNECTTIMEOUT,5L);
        curl_easy_setopt(c,CURLOPT_NOSIGNAL,1L); curl_easy_setopt(c,CURLOPT_LOW_SPEED_LIMIT,128L);
        curl_easy_setopt(c,CURLOPT_LOW_SPEED_TIME,10L); curl_easy_setopt(c,CURLOPT_FOLLOWLOCATION,1L);
        curl_multi_add_handle(m,c); }
    int running=0; curl_multi_perform(m,&running);
    while(running){ int nfds=0; curl_multi_poll(m,nullptr,0,1000,&nfds); curl_multi_perform(m,&running); }
    for(size_t i=0;i<hs.size();++i){ long code=0; curl_easy_getinfo(hs[i],CURLINFO_RESPONSE_CODE,&code); codes[i]=code;
        if(code>=400 || out[i].empty()) fprintf(stderr,"SEC unavailable %ld %s\n",code,urls[i].c_str());
        curl_multi_remove_handle(m,hs[i]); curl_easy_cleanup(hs[i]); }
    curl_multi_cleanup(m); return out;
}
#else
static string http_get(const string& url,const string&){fprintf(stderr,"[NO_CURL] would GET %s\n",url.c_str());exit(2);}
static string http_get_sec(const string& url,const string& ua){ return http_get(url,ua); }
static vector<string> http_get_sec_many(const vector<string>& urls,const string& ua, vector<long>& codes){ vector<string> out; for(auto& u:urls){out.push_back(http_get_sec(u,ua)); codes.push_back(out.back().empty()?0:200);} return out; }
#endif

// ============================== embedded universe ===========================
// Extracted from dumb_china_factor.py (uploaded). Sectors verified via issuer
// pages; sector tags below are final (user-verified 2026-10-23).
struct EtfDef { const char* tic; const char* region; const char* sector; const char* source; };
static const EtfDef CHINA_ETFS[] = {
    // -- US-listed China ETFs (Yahoo tickers) --
    {"KSTR","CN_US","STAR Market 50 / hard tech","KraneShares SSE STAR50"},
    {"CNXT","CN_US","ChiNext growth / innovation","VanEk ChiNext"},
    {"KGRN","CN_US","ESG leaders broad","KraneShares MSCI China ESG"},
    {"KTEC","CN_US","STAR science & tech","KraneShares SSE STAR"},
    {"TCHI","CN_US","growth leaders tech-heavy","VanEk China Growth Leaders"},
    {"KWEB","CN_US","China internet","KraneShares CSI China Internet"},
    {"CHIQ","CN_US","consumer discretionary","Global X MSCI China Consumer Disc."},
    {"KURE","CN_US","healthcare","KraneShares MSCI China Healthcare"},
    {"FXI","CN_US","large-cap SOE value (financials/energy)","iShares China Large-Cap"},
    {"ASHR","CN_US","A-shares CSI 300 broad","Xtrackers Harvest CSI 300"},
    {"MCHI","CN_US","MSCI China all-cap broad","iShares MSCI China"},
    {"CXSE","CN_US","ex-SOE private sector broad","WisdomTree China ex-SOEs"},
    {"ASHS","CN_US","A-shares CSI 500 mid-small","Xtrackers Harvest CSI 500"},
    // -- mainland-listed (akshare/Sina source; Yahoo suffix) --
    {"510720.SS","CN_MAIN","SOE dividend value (fin/energy/ind)","Guotai SSE SOE Dividend 000151"},
    {"513120.SS","CN_MAIN","HK innovative medicines (CSI HK Innovative Medicines)","Gf Fund 港股创新药ETF"},
    {"513320.SS","CN_MAIN","Hang Seng New Economy broad growth","恒生新经济ETF"},
    {"513550.SS","CN_MAIN","HK Connect 50 large-cap broad","港股通50ETF"},
    {"515220.SS","CN_MAIN","coal (CSI Coal)","国泰中证煤炭ETF"},
    {"516310.SS","CN_MAIN","banks (E Fund Bank ETF)","易方达银行ETF"},
    {"516620.SS","CN_MAIN","film & television media (Guotai CSI Media)","国泰中证影视ETF"},
    {"159901.SZ","CN_MAIN","ChiNext broad growth","E Fund ChiNext ETF"},
    // -- HK-listed --
    {"3167.HK","CN_HK","new China sectors new-economy broad","CSOP S&P New China Sectors"},
    {"2841.HK","CN_HK","China gaming & esports","Global X China Gaming"},
    {"3189.HK","CN_HK","baijiu / white liquor (E Fund)","易方达白酒ETF"},
    {"2820.HK","CN_HK","China biotech","Global X China Biotech"},
    {"2826.HK","CN_HK","China cloud computing","Global X China Cloud Computing"},
    {"2809.HK","CN_HK","China clean energy","Global X China Clean Energy"},
    {"3134.HK","CN_HK","solar (China Southern / CSOP Solar)","南方太阳能ETF"},
    {"2845.HK","CN_HK","China EV & battery","Global X China EV & Battery"},
    {"2815.HK","CN_HK","China Little Giant: ChiNext/STAR innovative SMEs","Global X China Little Giant"},
    // -- global macro leg from dumb_china_factor.py --
    {"SPY","US","US large-cap broad","os-bloom/dumb_china_factor"},
    {"QQQ","US","US Nasdaq 100 growth","os-bloom/dumb_china_factor"},
    {"GLD","US","gold bullion","os-bloom/dumb_china_factor"},
    {"SLV","US","silver bullion","os-bloom/dumb_china_factor"},
    {"COPX","US","copper miners","os-bloom/dumb_china_factor"},
    {"NIKL","US","nickel miners","os-bloom/dumb_china_factor"},
};
static const int N_CHINA_ETFS=sizeof(CHINA_ETFS)/sizeof(EtfDef);

// ============================== schema ======================================
static void ensure_schema(Db& db){
    db.exec(R"(CREATE TABLE IF NOT EXISTS etfs(
        ticker TEXT PRIMARY KEY, region TEXT, sector TEXT, source TEXT);
    CREATE TABLE IF NOT EXISTS prices(
        ticker TEXT, date TEXT, close REAL, adjclose REAL, volume REAL, shares REAL,
        PRIMARY KEY(ticker,date)) WITHOUT ROWID;
    CREATE INDEX IF NOT EXISTS ix_prices_date ON prices(date);
    CREATE TABLE IF NOT EXISTS fundamentals(
        ticker TEXT, ffyear INTEGER, bm REAL, op REAL, inv REAL, shares REAL,
        PRIMARY KEY(ticker,ffyear)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS edgar_unavailable(
        ticker TEXT PRIMARY KEY, reason TEXT, status INTEGER) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS rf(date TEXT PRIMARY KEY, rf REAL) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS factors(
        date TEXT PRIMARY KEY, mkt REAL, smb REAL, hml REAL, rmw REAL, cma REAL, umd REAL) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS factor_momentum_cn(
        date TEXT, horizon INTEGER, factor TEXT, pct REAL,
        PRIMARY KEY(date,horizon,factor)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS factor_momentum_us(
        date TEXT, horizon INTEGER, factor TEXT, pct REAL,
        PRIMARY KEY(date,horizon,factor)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS factor_momentum_eu(
        date TEXT, horizon INTEGER, factor TEXT, pct REAL,
        PRIMARY KEY(date,horizon,factor)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS xsec_mean_beta_cn(
        date TEXT, horizon INTEGER, factor TEXT, beta REAL,
        PRIMARY KEY(date,horizon,factor)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS xsec_mean_beta_us(
        date TEXT, horizon INTEGER, factor TEXT, beta REAL,
        PRIMARY KEY(date,horizon,factor)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS xsec_mean_beta_eu(
        date TEXT, horizon INTEGER, factor TEXT, beta REAL,
        PRIMARY KEY(date,horizon,factor)) WITHOUT ROWID;
    CREATE TABLE IF NOT EXISTS spearman_rho(
        date TEXT, region TEXT, ticker TEXT, lookback INTEGER, factor TEXT, rho REAL,
        PRIMARY KEY(date,region,ticker,lookback,factor)) WITHOUT ROWID;)");
}

// ============================== fetch =======================================
static void upsert_etf(Db& db, const string& tic, const string& reg, const string& sec, const string& src){
    Db::Stmt st; st.prep(db.h,"INSERT OR REPLACE INTO etfs VALUES(?1,?2,?3,?4)");
    st.bind(1,tic);st.bind(2,reg);st.bind(3,sec);st.bind(4,src); sqlite3_step(st.s); }
static void insert_price(Db::Stmt& st,const string& tic,const string& d,double c,double a,double v,double sh){
    st.reset(); st.bind(1,tic);st.bind(2,d);st.bind(3,c);st.bind(4,a);st.bind(5,v);st.bind(6,sh); sqlite3_step(st.s); }

struct YahooSeries { string tic, region; int years=0; vector<double> ts, cl, ac; };
static YahooSeries fetch_yahoo_series(const string& tic,int years,const string& region){ // Yahoo adjusted close for every region
    // Yahoo chart URLs do not resolve the preferred-share caret symbols in
    // the F6 universe. Mark them unavailable without spending a 90s timeout.
    if(tic.find('^')!=string::npos){
        fprintf(stderr,"%s %s: unavailable (unsupported Yahoo symbol)\n",region.c_str(),tic.c_str());
        return {tic,region,years,{},{},{}};
    }
    // Keep requests deliberately below the provider's burst threshold. A five
    // year chart is preferred; if the response is empty or materially short,
    // retry the same ticker once at the supported three-year fallback.
    auto load=[&](int span){
        string js=http_get("https://query1.finance.yahoo.com/v8/finance/chart/"+tic+"?interval=1d&range="+to_string(span)+"y","BYOT/FF_Rotation");
        vector<double> ts=jarr(js,"timestamp"), cl=jarr(js,"close"), ac;
        size_t ap=js.find("\"adjclose\"");
        if(ap!=string::npos){ size_t in=js.find("adjclose",ap+10); if(in!=string::npos) ac=jarr(js.substr(in),"adjclose"); }
        if(ac.size()<ts.size()) ac=cl;
        return tuple<vector<double>,vector<double>,vector<double>>(move(ts),move(cl),move(ac));
    };
    auto [ts,cl,ac]=load(years);
    if(years==5 && ts.size()<900){
        this_thread::sleep_for(chrono::milliseconds(750));
        tie(ts,cl,ac)=load(3);
        years=3;
    }
    if(ts.empty()||cl.empty()){ fprintf(stderr,"%s %s: unavailable\n",region.c_str(),tic.c_str()); return {tic,region,years,{},{},{}}; }
    fprintf(stderr,"%s %s: %zu days (%dy)\n",region.c_str(),tic.c_str(),ts.size(),years);
    this_thread::sleep_for(chrono::milliseconds(250));
    return {tic,region,years,move(ts),move(cl),move(ac)};
}
static void persist_yahoo(Db& db,const YahooSeries& series){
    const auto& ts=series.ts; const auto& cl=series.cl; const auto& ac=series.ac;
    if(ts.empty()||cl.empty()) return;
    Db::Stmt st; st.prep(db.h,"INSERT OR REPLACE INTO prices VALUES(?1,?2,?3,?4,?5,?6)");
    for(size_t i=0;i<ts.size();++i){ time_t tt=(time_t)ts[i]; char buf[16]; strftime(buf,16,"%Y-%m-%d",gmtime(&tt));
        double adj=i<ac.size()?ac[i]:cl[i]; insert_price(st,series.tic,buf,cl[i],adj,NAN,NAN); }
}
static bool has_price_history(Db& db,const string& tic,int years){
    Db::Stmt q; q.prep(db.h,"SELECT COUNT(*) FROM prices WHERE ticker=?1"); q.bind(1,tic);
    // A non-empty history is an atomically committed provider result. Short
    // series are valid for recent IPOs and the documented 3y fallback; only
    // symbols with no stored observations should be retried.
    return q.step() && sqlite3_column_int64(q.s,0)>0;
}
static void fetch_rf(Db& db,int years){ // Treasury 1-Mo
    Db::Stmt st; st.prep(db.h,"INSERT OR REPLACE INTO rf VALUES(?1,?2)");
    time_t nw=time(nullptr); tm* gg=gmtime(&nw);
    for(int yr=gg->tm_year+1900-years;yr<=gg->tm_year+1900;++yr){ char yb[8]; snprintf(yb,8,"%d",yr);
        string csv=http_get(string("https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/")+
            yb+"/all?type=daily_treasury_yield_curve&field_tdr_date_value="+yb+"&page&_format=csv","FF_Rotation/1.0");
        istringstream in(csv); string line; bool hdr=true; int dc=-1,rc=-1;
        while(getline(in,line)){ auto c=split_csv(line);
            if(hdr){ hdr=false; for(size_t i=0;i<c.size();++i){ string h=trim(c[i]);
                if(h=="Date")dc=(int)i; if((h=="1 Mo"||h=="1 Yr")&&rc<0)rc=(int)i; } if(rc<0)rc=1; continue; }
            if(dc<0||(size_t)dc>=c.size())continue; double y=to_d(c[rc]); if(isnan(y))continue;
            st.reset(); st.bind(1,ymd_norm(c[dc])); st.bind(2,y/100.0/252.0); sqlite3_step(st.s); } }
    fprintf(stderr,"RF done\n");
}
static void fetch_osbloom_universe(Db& db){
    // cleyfe/os-bloom config.yaml -- actual schema (verified):
    //   - title: US SECTORS | EUROPE SECTORS
    //     rows:
    //       - { symbol: XLK, name: "Technology", yahoo: XLK }
    //       - { symbol: EXV3, name: "Technology", yahoo: EXV3.DE }
    // Section title -> region; name -> sector label; yahoo -> fetch ticker.
    string y=http_get("https://raw.githubusercontent.com/cleyfe/os-bloom/main/config.yaml","FF_Rotation/1.0");
    istringstream in(y); string line, sec_up;
    regex rsym(R"(\bsymbol:\s*([A-Za-z0-9.]+))"), rname(R"(\bname:\s*"([^"]+))"), ryh(R"(\byahoo:\s*([A-Za-z0-9.]+))");
    int n=0;
    while(getline(in,line)){
        size_t tp=line.find("title:");
        if(tp!=string::npos){ string s=trim(line.substr(tp+6));
            s.erase(remove(s.begin(),s.end(),'"'),s.end());
            sec_up=s; transform(sec_up.begin(),sec_up.end(),sec_up.begin(),::toupper); continue; }
        if(line.find("symbol:")==string::npos) continue;
        smatch m1,m2,m3;
        string sym=regex_search(line,m1,rsym)?m1[1].str():""; if(sym.empty()) continue;
        string nm =regex_search(line,m2,rname)?m2[1].str():"";
        string yh =regex_search(line,m3,ryh )?m3[1].str():sym;
        string reg="US";
        if(sec_up.find("EUROPE")!=string::npos||sec_up.find("EU ")!=string::npos) reg="EU";
        upsert_etf(db,yh,reg,nm.empty()?sym:nm,"cleyfe/os-bloom config.yaml");
        ++n;
    }
    fprintf(stderr,"os-bloom universe: %d rows (%s)\n",n,
        n==31?"expected 11 US + 19 EU + 1 EXI5 real-estate": "WARNING: count differs from expected 31");
}
static void fetch_f6_us_universe(Db& db){
    const char* home=getenv("QUANTPILOT_HOME");
    string dir=home&&*home?home:string(getenv("HOME")?getenv("HOME"):".")+"/.quantpilot";
    string path=dir+"/market.db";
    sqlite3* src=nullptr;
    if(sqlite3_open_v2(path.c_str(),&src,SQLITE_OPEN_READONLY,nullptr)!=SQLITE_OK){
        if(src)sqlite3_close(src);
        fprintf(stderr,"F6 WRLD: no readable market database at %s\n",path.c_str()); return;
    }
    const char* sql="SELECT symbol FROM universe WHERE snapshot_ts=(SELECT MAX(snapshot_ts) FROM universe) "
                    "AND country='United States' AND symbol IS NOT NULL AND symbol<>'' ORDER BY symbol";
    sqlite3_stmt* st=nullptr;
    if(sqlite3_prepare_v2(src,sql,-1,&st,nullptr)!=SQLITE_OK){
        fprintf(stderr,"F6 WRLD: universe query unavailable\n"); sqlite3_close(src); return;
    }
    int n=0; while(sqlite3_step(st)==SQLITE_ROW){
        string tic=(const char*)sqlite3_column_text(st,0);
        upsert_etf(db,tic,"REG_US","F6 WRLD United States","QuantPilot universe snapshot"); ++n;
    }
    sqlite3_finalize(st); sqlite3_close(src);
    fprintf(stderr,"F6 WRLD United States: %d regression tickers\n",n);
}
static string sec_contact(){
    const char* env=getenv("QUANTPILOT_SEC_CONTACT");
    if(env&&*env) return env;
    ifstream in("/tmp/quantpilot/config.json"); string s((istreambuf_iterator<char>(in)),{});
    smatch m; regex r("\\\"sec_contact\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"");
    return regex_search(s,m,r)?m[1].str():string();
}
static void fetch_fundamentals_edgar(Db& db,int years,int ynow){
    // SEC EDGAR, 5y window: BE -> bm (vs latest close), OperatingIncomeLoss -> op,
    // Assets growth -> inv, EntityCommonStockSharesOutstanding -> shares.
    string contact=sec_contact();
    string ua="BYOT/FF_Rotation"+(contact.empty()?string():string(" ")+contact);
    string tj=http_get("https://www.sec.gov/files/company_tickers.json",ua);
    map<string,string> cik; size_t p=0;
    while((p=tj.find("\"ticker\":",p))!=string::npos){ string tk=jstr(tj.substr(p),"ticker"),c=jstr(tj.substr(p),"cik_str");
        if(!tk.empty()) cik[tk]=c; p+=9; }
    Db::Stmt ins; ins.prep(db.h,"INSERT OR REPLACE INTO fundamentals VALUES(?1,?2,?3,?4,?5,?6)");
    Db::Stmt seen; seen.prep(db.h,"SELECT 1 FROM fundamentals WHERE ticker=?1 LIMIT 1");
    Db::Stmt bad; bad.prep(db.h,"SELECT 1 FROM edgar_unavailable WHERE ticker=?1 LIMIT 1");
    Db::Stmt mark_bad; mark_bad.prep(db.h,"INSERT OR REPLACE INTO edgar_unavailable VALUES(?1,?2,?3)");
    Db::Stmt q; q.prep(db.h,"SELECT e.ticker FROM etfs e WHERE e.region='REG_US' AND EXISTS (SELECT 1 FROM prices p WHERE p.ticker=e.ticker) ORDER BY e.ticker");
    vector<string> us; while(q.step()) us.push_back(q.colS(0));
    int mapped=0, written=0;
    db.tx_begin();
    for(size_t ui=0;ui<us.size();++ui){ auto& tic=us[ui];
        seen.reset(); seen.bind(1,tic); if(seen.step()) continue;
        bad.reset(); bad.bind(1,tic); if(bad.step()) continue;
        string C=cik.count(tic)?cik[tic]:"";
        if(C.empty()){ if((ui+1)%25==0){db.tx_commit();db.tx_begin();} continue; }
        ++mapped;
        string pad(10-C.size(),'0'), base="https://data.sec.gov/api/xbrl/companyconcept/CIK"+pad+C+"/us-gaap/";
        map<int,double> BE,OI,AS,SH;
        auto grab=[&](const string& url,map<int,double>& out){
            string j=http_get_sec(url,ua); size_t pp=0;
            while((pp=j.find("\"fy\":",pp))!=string::npos){
                string fp=jstr(j.substr(pp),"fp"),fm=jstr(j.substr(pp),"form");
                int fy=(int)to_d(jstr(j.substr(pp),"fy")); double v=to_d(jstr(j.substr(pp),"val"));
                if((fm=="10-K"||fp=="FY")&&!isnan(v)) out[fy]=v; pp+=5; } };
        string shares="https://data.sec.gov/api/xbrl/companyconcept/CIK"+pad+C+"/dei/EntityCommonStockSharesOutstanding.json";
        vector<string> urls={base+"StockholdersEquity.json",base+"OperatingIncomeLoss.json",base+"Assets.json",shares};
        vector<long> codes; vector<string> docs=http_get_sec_many(urls,ua,codes);
        bool unavailable=false; long status=0;
        for(long code:codes){ if(code) status=code; if(code==404 || code==429 || code>=500 || code==0) unavailable=true; }
        if(unavailable){ mark_bad.reset(); mark_bad.bind(1,tic); mark_bad.bind(2,"SEC facts unavailable"); mark_bad.bind(3,status); sqlite3_step(mark_bad.s); db.tx_commit(); db.tx_begin(); fprintf(stderr,"EDGAR skip %zu/%zu %s unavailable\n",ui+1,us.size(),tic.c_str()); continue; }
        for(size_t di=0;di<docs.size();++di){
            size_t pp=0; while((pp=docs[di].find("\"fy\":",pp))!=string::npos){
                string fp=jstr(docs[di].substr(pp),"fp"),fm=jstr(docs[di].substr(pp),"form");
                int fy=(int)to_d(jstr(docs[di].substr(pp),"fy")); double v=to_d(jstr(docs[di].substr(pp),"val"));
                if((fm=="10-K"||fp=="FY")&&!isnan(v)){ if(di==0)BE[fy]=v; else if(di==1)OI[fy]=v; else if(di==2)AS[fy]=v; else SH[fy]=v; }
                pp+=5;
            }
        }
        Db::Stmt lc; lc.prep(db.h,"SELECT adjclose FROM prices WHERE ticker=?1 ORDER BY date DESC LIMIT 1");
        lc.bind(1,tic); double lastpx=0; if(lc.step()) lastpx=lc.colD(0);
        for(auto&kv:BE){ int fy=kv.first; if(fy<ynow-years-1||fy>ynow) continue;
            double be=kv.second; if(be<=0) continue;
            double sh=SH.count(fy)?SH[fy]:(SH.empty()?0:SH.rbegin()->second);
            double bm=(sh>0&&lastpx>0)?be/(sh*lastpx):NAN;
            double op=OI.count(fy)?OI[fy]/be:NAN, inv=NAN;
            auto a1=AS.find(fy),a0=AS.find(fy-1);
            if(a1!=AS.end()&&a0!=AS.end()&&a0->second>0) inv=a1->second/a0->second-1.0;
            ins.reset(); ins.bind(1,tic); ins.bind(2,(double)fy);
            if(!isnan(bm))ins.bind(3,bm); if(!isnan(op))ins.bind(4,op); if(!isnan(inv))ins.bind(5,inv);
            if(sh>0){ins.bind(6,sh); ++written;} sqlite3_step(ins.s); }
        db.tx_commit(); db.tx_begin();
        fprintf(stderr,"EDGAR progress %zu/%zu %s rows=%d\n",ui+1,us.size(),tic.c_str(),written);
    }
    db.tx_commit();
    fprintf(stderr,"fundamentals (EDGAR, %dy window): %d mapped tickers, %d rows, contact=%s\n",
            years,mapped,written,contact.empty()?"missing":"configured");
}

static int mode_fetch(int argc,char**argv){
    string dbp="ff_rotation.db"; bool skip_embedded=false; int years=5; int workers=4;
    for(int i=2;i<argc;++i){ string a=argv[i];
        if(a=="--db") dbp=argv[++i]; else if(a=="--skip-embedded") skip_embedded=true;
        else if(a=="--years") years=stoi(argv[++i]); else if(a=="--workers") workers=max(1,min(8,stoi(argv[++i]))); }
    time_t now=time(nullptr); tm* g=gmtime(&now); int ynow=g->tm_year+1900;
    char cb[16]; snprintf(cb,16,"%04d-%02d-%02d",ynow-years,min(g->tm_mon+1,12),min(g->tm_mday,28));
    string cutoff=cb;
    #ifndef NO_CURL
    curl_global_init(CURL_GLOBAL_DEFAULT);
    #endif
    Db db; db.open(dbp); ensure_schema(db);
    db.tx_begin();
    if(!skip_embedded) for(int i=0;i<N_CHINA_ETFS;++i)
        upsert_etf(db,CHINA_ETFS[i].tic,CHINA_ETFS[i].region,CHINA_ETFS[i].sector,CHINA_ETFS[i].source);
    fetch_osbloom_universe(db);
    fetch_f6_us_universe(db);
    db.tx_commit();
    Db::Stmt q; q.prep(db.h,"SELECT ticker,region FROM etfs");
    vector<pair<string,string>> tks;
    while(q.step()) tks.push_back({q.colS(0),q.colS(1)});
    int reused=0, fetched=0; vector<pair<string,string>> pending;
    for(auto&[t,r]:tks){
        if(has_price_history(db,t,years)){ ++reused; continue; }
        pending.push_back({t,r});
    }
    for(size_t at=0;at<pending.size();at+=(size_t)workers){
        vector<future<YahooSeries>> jobs;
        size_t stop=min(pending.size(),at+(size_t)workers);
        for(size_t i=at;i<stop;++i)
            jobs.push_back(async(launch::async,fetch_yahoo_series,pending[i].first,years,pending[i].second));
        for(auto& job:jobs){
            YahooSeries series=job.get();
            db.tx_begin(); persist_yahoo(db,series); db.tx_commit(); ++fetched;
        }
    }
    fprintf(stderr,"price history: reused=%d fetched=%d workers=%d frequency=1d\n",reused,fetched,workers);
    db.tx_begin(); fetch_rf(db,years); db.tx_commit(); fetch_fundamentals_edgar(db,years,ynow);
    return 0;
}

// ============================== build: canonical FF6 ========================
struct Sec { string tic,region; vector<double> adj,px,ret,sh;
             map<int,array<double,3>> fund; double sh_latest=0; };
static double percentile(vector<double> v,double p){ if(v.empty())return 0;
    sort(v.begin(),v.end()); double x=p/100*(v.size()-1); int i=(int)floor(x),f=x-i;
    return v[i]*(1-f)+v[min(i+1,(int)v.size()-1)]*f; }
static int month_of(const string& d){return stoi(d.substr(5,2));} static int year_of(const string& d){return stoi(d.substr(0,4));}
struct VW { vector<int> idx; vector<double> w;
    void build(const vector<pair<int,double>>& c){idx.clear();w.clear();for(auto&p:c){idx.push_back(p.first);w.push_back(p.second);}}
    double step(const vector<Sec>& S,int t){ double num=0,den=0;
        for(size_t j=0;j<idx.size();++j){ double r=S[idx[j]].ret[t];
            if(!isnan(r)&&isfinite(r)){num+=w[j]*r;den+=w[j];w[j]*=(1.0+r);} }
        return den>0?num/den:NAN; } };
static int mode_build(int argc,char**argv){
    string dbp="ff_rotation.db"; for(int i=2;i<argc;++i){string a=argv[i]; if(a=="--db")dbp=argv[++i];}
    Db db; db.open(dbp); ensure_schema(db);
    // calendar
    vector<string> dates; { Db::Stmt q; q.prep(db.h,"SELECT DISTINCT date FROM prices ORDER BY date");
        while(q.step()) dates.push_back(q.colS(0)); }
    int n=(int)dates.size(); unordered_map<string,int> didx; for(int i=0;i<n;++i) didx[dates[i]]=i;
    // panels
    vector<Sec> S; unordered_map<string,int> ix;
    { Db::Stmt q; q.prep(db.h,"SELECT e.ticker,e.region FROM etfs e WHERE e.region!='REG_US' OR (EXISTS (SELECT 1 FROM prices p WHERE p.ticker=e.ticker) AND EXISTS (SELECT 1 FROM fundamentals f WHERE f.ticker=e.ticker))");
      while(q.step()){ Sec s; s.tic=q.colS(0); s.region=q.colS(1);
          s.adj.assign(n,NAN); s.px.assign(n,NAN); s.sh.assign(n,NAN);
          ix[s.tic]=S.size(); S.push_back(move(s)); } }
    { Db::Stmt q; q.prep(db.h,"SELECT ticker,date,close,adjclose,shares FROM prices");
      long done=0;
      while(q.step()){ auto it=ix.find(q.colS(0)); if(it==ix.end())continue; auto di=didx.find(q.colS(1));
          if(di==didx.end())continue; Sec& s=S[it->second]; int i=di->second;
          s.px[i]=q.isNull(2)?NAN:q.colD(2); s.adj[i]=q.isNull(3)?NAN:q.colD(3);
          s.sh[i]=q.isNull(4)?NAN:q.colD(4);
          if(++done%500000==0) fprintf(stderr,"prices %ld\r",done); } }
    for(auto& s:S){ s.ret.assign(n,NAN);
        for(int i=1;i<n;++i) if(!isnan(s.adj[i])&&!isnan(s.adj[i-1])&&s.adj[i-1]>0)
            s.ret[i]=s.adj[i]/s.adj[i-1]-1.0; }
    { Db::Stmt q; q.prep(db.h,"SELECT ticker,ffyear,bm,op,inv,shares FROM fundamentals");
      while(q.step()){ auto it=ix.find(q.colS(0)); if(it==ix.end())continue; Sec& s=S[it->second];
          int fy=(int)q.colD(1); s.fund[fy]={q.isNull(2)?NAN:q.colD(2),
              q.isNull(3)?NAN:q.colD(3),q.isNull(4)?NAN:q.colD(4)};
          if(!q.isNull(5)&&q.colD(5)>0) s.sh_latest=q.colD(5); } }
    for(auto& s:S) for(auto& v:s.sh) if(isnan(v)&&s.sh_latest>0) v=s.sh_latest;
    map<string,double> RF; { Db::Stmt q; q.prep(db.h,"SELECT date,rf FROM rf");
        while(q.step()) RF[q.colS(0)]=q.colD(1); }
    // assignment
    auto fund_at=[](const Sec& s,int y)->array<double,3>{ auto it=s.fund.upper_bound(y);
        if(it==s.fund.begin())return{NAN,NAN,NAN}; return (--it)->second; };
    struct Grid{vector<pair<int,double>> c[2][3];}; struct Asn{Grid bm,op,inv;vector<pair<int,double>> all;};
    auto assign=[&](int A,int year){ vector<double> MEs,BMs,OPs,INVs; vector<int> us;
        for(int i=0;i<(int)S.size();++i){ auto&s=S[i];
            if(s.region!="REG_US"||isnan(s.px[A])||isnan(s.sh[A])||s.px[A]<=0||s.sh[A]<=0)continue;
            auto f=fund_at(s,year-1); if(isnan(f[0])||f[0]<=0)continue;
            us.push_back(i); MEs.push_back(s.px[A]*s.sh[A]); BMs.push_back(f[0]);
            if(!isnan(f[1])) OPs.push_back(f[1]);
            if(!isnan(f[2])) INVs.push_back(f[2]); }
        double med=percentile(MEs,50),b30=percentile(BMs,30),b70=percentile(BMs,70),
               o30=percentile(OPs,30),o70=percentile(OPs,70),i30=percentile(INVs,30),i70=percentile(INVs,70);
        Asn A3; for(int k:us){ auto&s=S[k]; double ME=s.px[A]*s.sh[A]; auto f=fund_at(s,year-1);
            int sz=ME<=med?0:1; auto s3=[](double x,double lo,double hi){return x<=lo?0:(x>=hi?2:1);};
            A3.bm.c[sz][s3(f[0],b30,b70)].push_back({k,ME});
            if(!isnan(f[1])) A3.op.c[sz][s3(f[1],o30,o70)].push_back({k,ME});
            if(!isnan(f[2])) A3.inv.c[sz][s3(f[2],i30,i70)].push_back({k,ME});
            A3.all.push_back({k,ME}); }
        return A3; };
    vector<int> monthend; for(int i=1;i<n;++i) if(month_of(dates[i])!=month_of(dates[i-1])) monthend.push_back(i-1);
    monthend.push_back(n-1);
    bool haveUMD=((int)monthend.size()>=13);   // 12 full months before first 12-2 sort
    if(!haveUMD) fprintf(stderr,"WARNING: fetched history < 13 month-ends -> UMD dropped, running FF5\n");
    map<int,int> juneA; for(int i=0;i<n;++i) if(month_of(dates[i])==6) juneA[year_of(dates[i])]=i;
    Asn A3; int cell_from=INT_MAX,cell_year=-1; VW vwMkt,vwBM[2][3],vwOP[2][3],vwINV[2][3],vwW,vwL; int um_from=INT_MAX, umd_start=INT_MAX;
    Db::Stmt ins; ins.prep(db.h,"INSERT OR REPLACE INTO factors VALUES(?1,?2,?3,?4,?5,?6,?7)");
    db.tx_begin();
    for(int t=0;t<n;++t){ int y=year_of(dates[t]);
        if(month_of(dates[t])==7&&juneA.count(y)&&juneA[y]<t&&cell_year!=y){
            A3=assign(juneA[y],y); cell_from=juneA[y]+1; cell_year=y; vwMkt.build(A3.all);
            for(int a=0;a<2;++a)for(int b=0;b<3;++b){ vwBM[a][b].build(A3.bm.c[a][b]);
                vwOP[a][b].build(A3.op.c[a][b]); vwINV[a][b].build(A3.inv.c[a][b]); }
            fprintf(stderr,"assign %d-06: %zu names\n",y,A3.all.size()); }
        auto mi=find(monthend.begin(),monthend.end(),t);
        if(mi!=monthend.end()){ int k=(int)(mi-monthend.begin());
            if(k>=12){ int q=monthend[k-12],psk=monthend[k-1];
                vector<pair<double,pair<int,double>>> mom;
                for(int i=0;i<(int)S.size();++i){ auto&s=S[i];
                    if(isnan(s.adj[q])||isnan(s.adj[psk])||s.adj[q]<=0)continue;
                    int qm=psk; while(qm>=0&&isnan(s.px[qm]))--qm; if(qm<0||isnan(s.sh[qm]))continue;
                    mom.push_back({s.adj[psk]/s.adj[q]-1.0,{i,s.px[qm]*s.sh[qm]}}); }
                if(mom.size()>=10){ sort(mom.begin(),mom.end()); int cut=max(1,(int)(mom.size()*0.3));
                    vector<pair<int,double>> L,W;
                    for(int j=0;j<cut;++j)L.push_back(mom[j].second);
                    for(int j=(int)mom.size()-cut;j<(int)mom.size();++j)W.push_back(mom[j].second);
                    vwL.build(L); vwW.build(W); um_from=t+1; umd_start=min(umd_start,t+1); } } }
        if(t<cell_from)continue;
        if(haveUMD&&t<umd_start)continue;   // publish only rows with UMD populated
        double mkt=vwMkt.step(S,t); if(isnan(mkt))continue;
        double SL=vwBM[0][0].step(S,t),SM=vwBM[0][1].step(S,t),SH=vwBM[0][2].step(S,t),
               BL=vwBM[1][0].step(S,t),BM=vwBM[1][1].step(S,t),BH=vwBM[1][2].step(S,t);
        double smb=(SL+SM+SH)/3.0-(BL+BM+BH)/3.0, hml=(SH+BH)/2.0-(SL+BL)/2.0;
        double rmw=(vwOP[0][2].step(S,t)+vwOP[1][2].step(S,t))/2.0-(vwOP[0][0].step(S,t)+vwOP[1][0].step(S,t))/2.0;
        double cma=(vwINV[0][0].step(S,t)+vwINV[1][0].step(S,t))/2.0-(vwINV[0][2].step(S,t)+vwINV[1][2].step(S,t))/2.0;
        double umd=NAN; if(t>=um_from){ double w=vwW.step(S,t),l=vwL.step(S,t);
            if(!isnan(w)&&!isnan(l))umd=w-l; }
        double rf=RF.count(dates[t])?RF[dates[t]]:0.0;
        ins.reset(); ins.bind(1,dates[t]); ins.bind(2,mkt-rf); ins.bind(3,smb); ins.bind(4,hml);
        ins.bind(5,rmw); ins.bind(6,cma); if(!isnan(umd))ins.bind(7,umd); sqlite3_step(ins.s);
        if(t%500==0){ db.tx_commit(); db.tx_begin(); } }
    db.tx_commit(); fprintf(stderr,"build: %d days -> factors\n",n);
    return 0;
}

// ============================== rotate: latency-optimized ===================
static const int HOR[6]={7,14,28,62,128,256};
static const char* HORN[6]={"7d","14d","28d","62d","128d","256d"};
static const char* GNAME[3]={"cn","us","eu"};
static int region_group(const string& r){ // 0=CN 1=US 2=EU
    if(r.rfind("CN",0)==0) return 0;
    if(r=="US") return 1;
    if(r=="EU") return 2;
    return 1;                              // unmapped tickers ride with US
}
static inline bool solveK(const double* A,const double* b,double* x,int K){ // stack, K<=7
    double M[8][8], bb[8];
    for(int i=0;i<K;++i){ bb[i]=b[i]; for(int j=0;j<K;++j) M[i][j]=A[i*K+j];
        if(!isfinite(bb[i])) return false;
        for(int j=0;j<K;++j) if(!isfinite(M[i][j])) return false; }
    for(int c=0;c<K;++c){ int piv=c;
        for(int r=c+1;r<K;++r) if(fabs(M[r][c])>fabs(M[piv][c])) piv=r;
        if(!isfinite(M[piv][c])||fabs(M[piv][c])<1e-12) return false;
        if(piv!=c){ for(int j=c;j<K;++j) swap(M[piv][j],M[c][j]); swap(bb[piv],bb[c]); }
        for(int r=c+1;r<K;++r){ double m=M[r][c]/M[c][c]; if(m==0) continue;
            for(int j=c;j<K;++j) M[r][j]-=m*M[c][j]; bb[r]-=m*bb[c]; } }
    for(int r=K-1;r>=0;--r){ double s=bb[r]; for(int j=r+1;j<K;++j) s-=M[r][j]*x[j];
        if(!isfinite(M[r][r])||fabs(M[r][r])<1e-12) return false;
        x[r]=s/M[r][r]; if(!isfinite(x[r])) return false; }
    return true;
}
static int mode_rotate(int argc,char**argv){
    string dbp="ff_rotation.db"; bool full=false; double lambda=0.0; int t_override=-1;
    for(int i=2;i<argc;++i){ string a=argv[i];
        if(a=="--db")dbp=argv[++i]; else if(a=="--full")full=true;
        else if(a=="--lambda")lambda=stod(argv[++i]); else if(a=="--at")t_override=stoi(argv[++i]); }
    Db db; db.open(dbp); ensure_schema(db);
    vector<string> FN; { Db::Stmt q; q.prep(db.h,"PRAGMA table_info(factors)");
        while(q.step()) FN.push_back(q.colS(1)); }
    if(FN.empty()||FN[0]!="date"){ fprintf(stderr,"factors table missing\n"); return 1; }
    FN.erase(FN.begin());
    const int k=(int)FN.size(), K=k+1;
    if(k<4||k>6){ fprintf(stderr,"unexpected factor count %d\n",k); return 1; }
    vector<string> dates; { Db::Stmt q; q.prep(db.h,"SELECT date FROM factors ORDER BY date");
        while(q.step()) dates.push_back(q.colS(0)); }
    int n=(int)dates.size(); if(n<300){fprintf(stderr,"only %d factor days\n",n);return 1;}
    vector<vector<double>> F(n, vector<double>(k,0.0));
    { string sel="SELECT "; for(int i=0;i<k;++i){ if(i)sel+=","; sel+=FN[i]; } sel+=" FROM factors ORDER BY date";
      Db::Stmt q; q.prep(db.h,sel); int i=0;
      while(q.step()){ for(int f=0;f<k;++f){ double v=q.colD(f); F[i][f]=isnan(v)?0.0:v; } ++i; } }
    // ETF universe with region groups
    vector<string> etfs; vector<int> grp; vector<vector<int>> members(3);
    { Db::Stmt q; q.prep(db.h,"SELECT ticker,region FROM etfs WHERE region!='REG_US' ORDER BY ticker");
      while(q.step()){ int g=region_group(q.colS(1)); etfs.push_back(q.colS(0)); grp.push_back(g);
          members[g].push_back((int)etfs.size()-1); } }
    int m=(int)etfs.size();
    unordered_map<string,int> didx; for(int i=0;i<n;++i) didx[dates[i]]=i;
    vector<vector<double>> E(m, vector<double>(n,NAN));
    { Db::Stmt q; q.prep(db.h,
        "SELECT p.ticker,p.date,p.adjclose FROM prices p JOIN etfs e ON e.ticker=p.ticker WHERE e.region!=\'REG_US\' ORDER BY p.ticker,p.date");
      unordered_map<string,int> eix; for(int i=0;i<m;++i)eix[etfs[i]]=i;
      string last; int li=-1; double lp=NAN; long done=0;
      while(q.step()){ string tic=q.colS(0); auto di=didx.find(q.colS(1)); if(di==didx.end())continue;
          int ei=eix[tic]; if(tic!=last){ last=tic; li=-1; lp=NAN; }
          double c=q.colD(2); int i=di->second;
          if(li>=0&&li<i&&!isnan(lp)&&lp>0) for(int j=li+1;j<=i;++j) E[ei][j]=c/lp-1.0;
          li=i; lp=c;
          if(++done%500000==0)fprintf(stderr,"etf px %ld\r",done); } }
    // shared factor prefix sums (region-independent), one pass: .shift(1)
    vector<array<double,64>> Sxx(n); vector<vector<array<double,8>>> Sxy(m, vector<array<double,8>>(n));
    vector<array<double,8>> Slog(n+1);
    for(int f=0;f<k;++f){ Slog[0][f]=0; Slog[1][f]=log1p(max(-0.999999,F[0][f])); }
    Sxx[0].fill(0); for(int e=0;e<m;++e)Sxy[e][0].fill(0);
    for(int i=1;i<n;++i){
        double phi[8]={1,F[i-1][0],F[i-1][1],F[i-1][2],F[i-1][3],F[i-1][4],F[i-1][5]};
        Sxx[i]=Sxx[i-1]; for(int e=0;e<m;++e)Sxy[e][i]=Sxy[e][i-1];
        for(int f=0;f<k;++f)Slog[i+1][f]=Slog[i][f];
        for(int a=0;a<K;++a)for(int b=0;b<K;++b)Sxx[i][a*K+b]+=phi[a]*phi[b];
        for(int e=0;e<m;++e){ double y=E[e][i]; if(!isnan(y))for(int a=0;a<K;++a)Sxy[e][i][a]+=phi[a]*y; }
        for(int f=0;f<k;++f)Slog[i+1][f]+=log1p(max(-0.999999,F[i][f]));
    }
    int t_now=n-1;
    // per-region result buffers
    Db::Stmt im[3], ib[3];
    for(int g=0;g<3;++g){
        im[g].prep(db.h,("INSERT OR REPLACE INTO factor_momentum_"+string(GNAME[g])+" VALUES(?1,?2,?3,?4)").c_str());
        ib[g].prep(db.h,("INSERT OR REPLACE INTO xsec_mean_beta_"+string(GNAME[g])+" VALUES(?1,?2,?3,?4)").c_str());
    }
    auto eval=[&](int t, vector<vector<double>>& mom, vector<vector<vector<double>>>& beta){
        mom.assign(6, vector<double>(k,0.0));
        beta.assign(3, vector<vector<double>>(6, vector<double>(k,0.0)));
        for(int h=0;h<6;++h){ int L=HOR[h];
            if(t-L>=0) for(int f=0;f<k;++f) mom[h][f]=(exp(Slog[t][f]-Slog[t-L][f])-1.0)*100.0; }
        // parallel over ETFs: one task = all six horizons for one ETF, in its region
        vector<array<double,48>> bsum(3); // [g][h*8+f] horizon-factor sums (merged below)
        vector<array<int,48>> bcnt(3);    // valid OLS fits, excluding singular systems
        for(int g=0;g<3;++g){ bsum[g].fill(0.0); bcnt[g].fill(0); }
        auto worker=[&](int e, array<double,48>* loc, array<int,48>* counts){ double x[8];
            for(int h=0;h<6;++h){ int L=HOR[h]; if(t-L<1) continue; int a=t-L,b=t;
                double A[64],bb[8];
                for(int x2=0;x2<K*K;++x2)A[x2]=Sxx[b][x2]-Sxx[a][x2];
                for(int x2=0;x2<K;++x2)bb[x2]=Sxy[e][b][x2]-Sxy[e][a][x2];
                if(lambda!=0.0)for(int d=1;d<K;++d)A[d*K+d]+=lambda;
                if(!solveK(A,bb,x,K)) continue;
                for(int f=0;f<k;++f) if(isfinite(x[f+1])){
                    loc[grp[e]][h*8+f]+=x[f+1]; counts[grp[e]][h*8+f]++;
                }
            } };
        unsigned nt=min<unsigned>(thread::hardware_concurrency(),8);
        if(m>=16&&nt>1){ atomic<int> cnt{0}; vector<vector<array<double,48>>> part(nt, vector<array<double,48>>(3));
            vector<vector<array<int,48>>> npart(nt, vector<array<int,48>>(3));
            for(auto&pa:part)for(auto&a:pa)a.fill(0.0);
            for(auto&pa:npart)for(auto&a:pa)a.fill(0);
            vector<thread> th;
            for(unsigned j=0;j<nt;++j) th.emplace_back([&,j]{int e;
                while((e=cnt.fetch_add(1))<m) worker(e, part[j].data(), npart[j].data());});
            for(auto&x2:th)x2.join();
            for(unsigned j=0;j<nt;++j)for(int g=0;g<3;++g)for(int i=0;i<48;++i){ bsum[g][i]+=part[j][g][i]; bcnt[g][i]+=npart[j][g][i]; } }
        else for(int e=0;e<m;++e) worker(e, bsum.data(), bcnt.data());
        for(int g=0;g<3;++g){ if(members[g].empty()) continue;
            for(int h=0;h<6;++h)for(int f=0;f<k;++f){ int nfit=bcnt[g][h*8+f];
                beta[g][h][f]=nfit?bsum[g][h*8+f]/nfit:NAN; }
        }
    };
    auto grid=[&](const char* title,const char* rg,const vector<vector<double>>& tab,int prec){
        printf("-- %s [%s] | as_of=%s | factors=%d\n %-6s",title,rg,dates[t_now].c_str(),k,"horizon");
        for(int i=0;i<k;++i)printf(" | %12s",FN[i].c_str());
        printf("\n%s\n",string(6+15*(k+1),'-').c_str());
        for(int h=0;h<6;++h){ printf(" %-6s",HORN[h]);
            for(int f=0;f<k;++f)printf(" | %12.*f",prec,tab[h][f]); printf("\n"); }
        printf("\n"); };
    auto emit=[&](int t){ t_now=t;
        vector<vector<double>> mom; vector<vector<vector<double>>> beta;
        eval(t,mom,beta);
        db.tx_begin();
        for(int g=0;g<3;++g){ string rg=GNAME[g];
            if(members[g].empty()) continue;
            grid("FACTOR_MOMENTUM (pct)",rg.c_str(),mom,2);
            grid("XSEC_MEAN_BETA (mean beta, OLS)",rg.c_str(),beta[g],4);
            for(int h=0;h<6;++h)for(int f=0;f<k;++f){
                im[g].reset(); im[g].bind(1,dates[t]); im[g].bind(2,HOR[h]); im[g].bind(3,FN[f]); im[g].bind(4,mom[h][f]); sqlite3_step(im[g].s);
                ib[g].reset(); ib[g].bind(1,dates[t]); ib[g].bind(2,HOR[h]); ib[g].bind(3,FN[f]); ib[g].bind(4,beta[g][h][f]); sqlite3_step(ib[g].s); } }
        db.tx_commit(); };
    if(full){ for(int t=257;t<n;++t) emit(t); }
    else emit(t_override>=0?t_override:n-1);
    return 0;
}

// ============================== spearman report =============================
static void rankvec(const vector<double>& v, vector<double>& out){ // average ranks
    int n=(int)v.size(); vector<int> idx(n); iota(idx.begin(),idx.end(),0);
    sort(idx.begin(),idx.end(),[&](int a,int b){return v[a]<v[b];});
    out.assign(n,0.0);
    for(int i=0;i<n;){ int j=i; while(j<n&&v[idx[j]]==v[idx[i]]) ++j;
        double r=(i+j-1)/2.0+1.0; for(int q=i;q<j;++q) out[idx[q]]=r; i=j; }
}
static double spearman(const vector<double>& x,const vector<double>& y){ // Pearson on ranks, ties exact
    int n=(int)x.size(); if(n<20) return NAN;
    vector<double> rx,ry; rankvec(x,rx); rankvec(y,ry);
    double mx=0,my=0; for(int i=0;i<n;++i){mx+=rx[i];my+=ry[i];} mx/=n; my/=n;
    double sxy=0,sxx=0,syy=0;
    for(int i=0;i<n;++i){ double dx=rx[i]-mx,dy=ry[i]-my; sxy+=dx*dy; sxx+=dx*dx; syy+=dy*dy; }
    if(sxx<=0||syy<=0) return NAN;
    return sxy/sqrt(sxx*syy);
}
static int mode_spearman(int argc,char**argv){
    string dbp="ff_rotation.db", out="spearman_report.html"; int t_override=-1;
    for(int i=2;i<argc;++i){ string a=argv[i];
        if(a=="--db")dbp=argv[++i]; else if(a=="--out")out=argv[++i]; else if(a=="--at")t_override=stoi(argv[++i]); }
    Db db; db.open(dbp); ensure_schema(db);
    vector<string> FN; { Db::Stmt q; q.prep(db.h,"PRAGMA table_info(factors)");
        while(q.step()) FN.push_back(q.colS(1)); }
    if(FN.empty()||FN[0]!="date"){ fprintf(stderr,"factors table missing\n"); return 1; }
    FN.erase(FN.begin());
    const int k=(int)FN.size(); if(k<4||k>6){ fprintf(stderr,"unexpected factor count %d\n",k); return 1; }
    vector<string> dates; { Db::Stmt q; q.prep(db.h,"SELECT date FROM factors ORDER BY date");
        while(q.step()) dates.push_back(q.colS(0)); }
    int n=(int)dates.size(); if(n<300){fprintf(stderr,"only %d factor days\n",n);return 1;}
    const int T=t_override>=0?t_override:n-1;
    vector<vector<double>> F(n, vector<double>(k,NAN));   // NaN kept; skipped in pairs
    { string sel="SELECT "; for(int i=0;i<k;++i){ if(i)sel+=","; sel+=FN[i]; } sel+=" FROM factors ORDER BY date";
      Db::Stmt q; q.prep(db.h,sel); int i=0;
      while(q.step()){ for(int f=0;f<k;++f) F[i][f]=q.colD(f); ++i; } }
    vector<string> etfs, tags, fulls; vector<int> grp; vector<vector<int>> members(3);
    { Db::Stmt q; q.prep(db.h,"SELECT ticker,region,sector,source FROM etfs WHERE region!='REG_US' ORDER BY ticker");
      while(q.step()){ int g=region_group(q.colS(1)); etfs.push_back(q.colS(0)); grp.push_back(g);
          tags.push_back(q.colS(2)); fulls.push_back(q.colS(3));
          members[g].push_back((int)etfs.size()-1); } }
    int m=(int)etfs.size();
    unordered_map<string,int> didx; for(int i=0;i<n;++i) didx[dates[i]]=i;
    vector<vector<double>> E(m, vector<double>(n,NAN));
    { Db::Stmt q; q.prep(db.h,
        "SELECT p.ticker,p.date,p.adjclose FROM prices p JOIN etfs e ON e.ticker=p.ticker WHERE e.region!=\'REG_US\' ORDER BY p.ticker,p.date");
      unordered_map<string,int> eix; for(int i=0;i<m;++i)eix[etfs[i]]=i;
      string last; int li=-1; double lp=NAN;
      while(q.step()){ string tic=q.colS(0); auto di=didx.find(q.colS(1)); if(di==didx.end())continue;
          int ei=eix[tic]; if(tic!=last){ last=tic; li=-1; lp=NAN; }
          double c=q.colD(2); int i=di->second;
          if(li>=0&&li<i&&!isnan(lp)&&lp>0) for(int j=li+1;j<=i;++j) E[ei][j]=c/lp-1.0;
          li=i; lp=c; } }
    vector<double> rho(m*6*k, NAN);
    atomic<int> cnt{0};
    auto worker=[&](int e){ vector<double> xs,ys;
        for(int h=0;h<6;++h){ int L=HOR[h]; if(T-L<1) continue;
            for(int f=0;f<k;++f){ xs.clear(); ys.clear();
                for(int i=T-L+1;i<=T;++i){ double y=E[e][i], x=F[i-1][f];
                    if(!isnan(y)&&!isnan(x)){ xs.push_back(x); ys.push_back(y); } }
                if((int)xs.size()>=max(20,(int)(L*0.6)))
                    rho[(e*6+h)*k+f]=spearman(xs,ys);
            } } };
    unsigned nt=min<unsigned>(thread::hardware_concurrency(),8);
    if(m>=16&&nt>1){ vector<thread> th;
        for(unsigned j=0;j<nt;++j) th.emplace_back([&]{int e; while((e=cnt.fetch_add(1))<m) worker(e);});
        for(auto&x:th)x.join(); }
    else for(int e=0;e<m;++e) worker(e);
    Db::Stmt is; is.prep(db.h,"INSERT OR REPLACE INTO spearman_rho VALUES(?1,?2,?3,?4,?5,?6)");
    db.tx_begin();
    for(int e=0;e<m;++e)for(int h=0;h<6;++h)for(int f=0;f<k;++f) if(!isnan(rho[(e*6+h)*k+f])){
        is.reset(); is.bind(1,dates[T]); is.bind(2,GNAME[grp[e]]); is.bind(3,etfs[e]);
        is.bind(4,HOR[h]); is.bind(5,FN[f]); is.bind(6,rho[(e*6+h)*k+f]); sqlite3_step(is.s); }
    db.tx_commit();
    string H="<html><head><meta charset='utf-8'><style>"
      "body{font:13px sans-serif;margin:24px}h1{font-size:18px}h2{font-size:15px;margin-top:28px}"
      "table{border-collapse:collapse;font:11px monospace;margin-bottom:8px}"
      "th,td{border:1px solid #bbb;padding:2px 5px;text-align:right;white-space:nowrap}"
      "th[title],td[title]{cursor:help}.tag{text-align:left;font-family:sans-serif}"
      "th.grp{background:#eee;text-align:center}th.fac{background:#f7f7f7;text-align:center}"
      ".pos{color:#fff}.neg{color:#fff}</style></head><body>\n";
    H+="<h1>Raw Spearman correlation &mdash; ETF returns vs factor returns, by lookback (as of "+dates[T]+")</h1>\n";
    H+="<p>Windows end yesterday (.shift(1) alignment). Row labels are sector tags (British English); hover a row for the fund's full name. Hover a cell for ticker &middot; lookback &middot; factor.</p>\n";
    char buf[768];
    for(int g=0;g<3;++g){ if(members[g].empty()) continue;
        snprintf(buf,sizeof buf,"<h2>%s</h2>\n<table><thead><tr><th class='tag' rowspan='2'>Sector tag</th><th rowspan='2'>Ticker</th>",GNAME[g]); H+=buf;
        for(int h=0;h<6;++h){ snprintf(buf,sizeof buf,"<th class='grp' colspan='%d'>%s</th>",k,HORN[h]); H+=buf; }
        H+="</tr><tr>";
        for(int h=0;h<6;++h)for(int f=0;f<k;++f){ snprintf(buf,sizeof buf,"<th class='fac'>%s</th>",FN[f].c_str()); H+=buf; }
        H+="</tr></thead><tbody>\n";
        for(int e:members[g]){
            string full=fulls[e].empty()?etfs[e]:(etfs[e]+" &mdash; "+fulls[e]);
            if(!tags[e].empty()) full+=" ("+tags[e]+")";
            string escT=tags[e].empty()?etfs[e]:tags[e];
            snprintf(buf,sizeof buf,"<tr><th class='tag' title=\"%s\">%s</th><th title=\"%s\">%s</th>",
                     full.c_str(),escT.c_str(),full.c_str(),etfs[e].c_str()); H+=buf;
            for(int h=0;h<6;++h)for(int f=0;f<k;++f){
                double r=rho[(e*6+h)*k+f];
                if(isnan(r)){ H+="<td></td>"; continue; }
                double a=min(1.0,fabs(r))*0.85;
                snprintf(buf,sizeof buf,"<td class='%s' style='background:rgba(%d,%d,%d,%.2f)' title=\"%s &middot; %s &middot; %s: rho = %.4f\">%.2f</td>",
                         r>=0?"pos":"neg", r>=0?0:230, r>=0?110:60, r>=0?220:60, a,
                         etfs[e].c_str(),HORN[h],FN[f].c_str(),r,r); H+=buf;
            }
            H+="</tr>\n";
        }
        H+="</tbody></table>\n";
    }
    H+="</body></html>\n";
    ofstream fo(out); fo<<H; fo.close();
    fprintf(stderr,"spearman: %d tickers x 6 lookbacks x %d factors -> %s + spearman_rho table\n",m,k,out.c_str());
    return 0;
}

int main(int argc,char**argv){
    if(argc<2){ fprintf(stderr,"usage:\n ff_rotation fetch [--db ff_rotation.db] [--years 5]\n ff_rotation build [--db ff_rotation.db]\n"
        " ff_rotation rotate [--db ff_rotation.db] [--full] [--at idx] [--lambda 0]\n"
        " ff_rotation spearman [--db ff_rotation.db] [--out spearman_report.html] [--at idx]\n"); return 1; }
    string mode=argv[1];
    if(mode=="fetch") return mode_fetch(argc,argv);
    if(mode=="build") return mode_build(argc,argv);
    if(mode=="rotate") return mode_rotate(argc,argv);
    if(mode=="spearman") return mode_spearman(argc,argv);
    fprintf(stderr,"unknown mode %s\n",mode.c_str()); return 1;
}
