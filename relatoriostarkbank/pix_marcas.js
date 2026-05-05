// Chaves PIX das marcas (fonte: planilha "CC Starkbank - Walid", aba pagar 05/05).
// Atualize manualmente quando a planilha mudar.
// Match feito pelo nomeFantasia (lowercase, trim, sem acento).
window.PIX_MARCAS = (function(){
    var raw = {
        "kelly rodrigues":    "54697378000115",
        "anne blanc":         "43865993000177",
        "bella donna":        "11933990994",
        "barraca do willinha":"35322340000113",
        "evian":              "49238112000174",
        "petit enfant":       "60741324000102",
        "alcance jeans":      "pessoal@alcancejeans.com",
        "incentive moda":     "29780242000127",
        "imporio fitness":    "38280852000152",
        "arary":              "13495868000151",
        "begkids":            "44870711000192",
        "ezee":               "moda.ezee4@gmail.com",
        "tee fashion":        "24459412000152",
        "zeros confec":       "45180025000152",
        "sedanbi":            "39922297000188",
        "missmel":            "17974887000111",
        "oxigenio modas":     "54911296000121",
        "mafia fitness":      "41549988000120"
    };
    function norm(s){
        return String(s||"").toLowerCase().trim()
            .normalize("NFD").replace(/[̀-ͯ]/g,"");
    }
    var map = {};
    for (var k in raw) map[norm(k)] = raw[k];
    return {
        get: function(nome){ return map[norm(nome)] || ""; },
        norm: norm
    };
})();
