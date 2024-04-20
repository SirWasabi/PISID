public class Alerta {

    private String data;
    private String sala;
    private String sensor;
    private String leitura;

    private String tipo;
    private String mensagem;


    public Alerta(String data, String sala, String sensor, String leitura, String tipo, String mensagem) {
        this.data = data;
        this.sala = sala;
        this.sensor = sensor;
        this.leitura = leitura;
        this.tipo = tipo;
        this.mensagem = mensagem;
    }

    public String getData() { return this.data; }

    public String getSala() { return this.sala; }

    public String getSensor() { return this.sensor; }

    public String getTipo() { return this.tipo; }

    public String getLeitura() { return this.leitura; }

    public String getMensagem() { return this.mensagem; }

    public static void main(String[] args) {
        Alerta a = new Alerta("silly", "silly", "silly", "silly", "silly", "silly");
        System.out.println(a.getData());
    }

}
