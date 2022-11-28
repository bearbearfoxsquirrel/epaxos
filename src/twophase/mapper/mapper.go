package mapper

func Mapper(i, iS, iE float64, oS, oE int32) int32 {
	slope := 1.0 * float64(oE-oS) / (iE - iS)
	o := oS + int32((slope*(i-iS))+0.5)
	return o
}
