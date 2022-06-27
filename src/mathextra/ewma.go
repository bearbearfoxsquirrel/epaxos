package mathextra

func EwmaAdd(ewma float64, weight float64, ob float64) float64 {
	return (1-weight)*ewma + weight*ob
}
