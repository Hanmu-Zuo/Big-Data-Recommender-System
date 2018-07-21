
public class Driver {
	public static void main(String[] args) throws Exception {
		
		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();
		AverageRanking average = new AverageRanking();
		AllMovies allMovies = new AllMovies();
		WriteAverageRanking writeAverageRanking = new WriteAverageRanking(average.rankedMovie);

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurrenceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];
		String averageDir = args[6];
		String allMovieDir = args[7];
		String writeAverage = args[8];

		String[] path1 = {rawInput, writeAverage, userMovieListOutputDir};
		String[] path2 = {userMovieListOutputDir, coOccurrenceMatrixDir};
		String[] path3 = {coOccurrenceMatrixDir, normalizeDir};
		String[] path4 = {normalizeDir, rawInput, multiplicationDir};
		String[] path5 = {multiplicationDir, sumDir};
		String[] path6 = {rawInput, averageDir};
		String[] path7 = {rawInput, allMovieDir};
		String[] path8 = {allMovieDir, averageDir, writeAverage};

		dataDividerByUser.main(path1);

		average.main(path6);
		allMovies.main(path7);
		writeAverageRanking.main(path8);

		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		multiplication.main(path4);
		sum.main(path5);

	}

}
