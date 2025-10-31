class X{
        private byte a;
        private short b;
        private int c;
        private long d;
        
        private char e;
        private boolean f;
        
        private float g;
        private double h;
        
        private String s;

        
        public void print() {
                String data= " [a=" + a + ", b=" + b + ", c=" + c + ", d=" + d + ", e=" + e + ", f=" + f + ", g=" + g + ", h=" + h
                                + ", s=" + s + "]";
                System.out.println(data);
        }
        
        
        
        
}
public class PrimitiveTypesTest {
    public static void main(String[] args) {
            X x1=new X();
            x1.print();
    }
}
