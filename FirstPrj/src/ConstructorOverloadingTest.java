class Number{
        private int i;
        private int j;

        Number(){
        }

        Number(int i){
               this.i=i;
        }

        Number(int i,int j){
               this(i);//invokes another constructor of same class with one argument
               this.j=j;
        }
        
        void print(){
                System.out.println("i="+i+",j="+j);
        }
}


class ConstructorOverloadingTest{
        public static void main(String[] args){
                Number n1=new Number(5);
                Number n2=new Number(5,10);
                Number n3=new Number();
                n1.print();
                n2.print();
                n3.print();
        }
}

