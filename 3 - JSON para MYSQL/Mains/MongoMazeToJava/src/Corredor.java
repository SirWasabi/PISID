public class Corredor {

    private int salaOrigem;
    private int salaDestino;

    public Corredor(int so, int sd) {
        this.salaOrigem = so;
        this.salaOrigem = sd;
    }

    public boolean corredorIgual(int so, int sd) {
        return (this.salaOrigem == so && this.salaDestino == sd);
    }

    public int getSalaOrigem() {
        return this.salaOrigem;
    }

    public int getSalaDestino() {
        return this.salaDestino;
    }

}
