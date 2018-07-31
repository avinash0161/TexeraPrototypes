namespace TexeraOrleansPrototype {

    public class Tuple
    {
        public int id;
        public string region;
        /*
        public string country;
        public string item_type;
        public string sales_channel;
        public string order_priority;
        public string order_date;
        public int order_id;
        public string ship_date;
        */
        public int units_sold;
        public float unit_price;
        public float unit_cost;
        /*
        public float total_revenue;
        public float total_cost;
        public float total_profit;
        */
        public Tuple(int id,string[] list) 
        {
            this.id = id;
            region = list[0];
            units_sold = int.Parse(list[8]);
            unit_price = float.Parse(list[9]);
            unit_cost = float.Parse(list[10]);
        }
    }
    
}