# This file is part of rubintv_production.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""End-to-end test for the all-sky image and movie pipeline."""

import os
import tempfile
import unittest
from unittest.mock import MagicMock

from lsst.rubintv.production.allSky import DayAnimator
from lsst.rubintv.production.uploaders import MultiUploader, Uploader

TEST_DATA_DIR = os.path.join(os.path.dirname(__file__), "data", "all_sky")


class AllSkyPipelineTestCase(unittest.TestCase):
    """End-to-end test of the all-sky still-frame and movie pipeline.

    Drives ``DayAnimator(historical=True)`` on two real JPEG fixtures -- the
    one-shot path that ``AllSkyMovieChannel`` takes when replaying a
    closed-out day. Mocks the two uploaders (no S3 / GCS in unit tests);
    everything else is real: file discovery, EXIF date/time extraction,
    ImageMagick ``convert`` for crop / stretch / annotate, ``ffmpeg`` for
    the mp4, upload-filename construction, the uploader call contract.

    Fixture 1 carries an EXIF ``DateTime`` tag so the textItems-driven
    annotation branch in ``_convertAndAnnotate`` runs; fixture 2 has no
    EXIF so the ``textItems=None`` branch also runs in the same test.

    Asserts on file existence and the upload-call shape; pixel content and
    codec details are out of scope.

    Nothing skips on missing ``convert`` / ``ffmpeg`` / fonts -- the whole
    point is to catch a regression where the runtime image stops shipping
    these tools.
    """

    def testDayAnimatorHistorical(self) -> None:
        s3Uploader = MagicMock(spec=MultiUploader)
        epoUploader = MagicMock(spec=Uploader)
        dayObsInt = 20260501

        with tempfile.TemporaryDirectory() as tmp:
            stillsDir = os.path.join(tmp, "stills")
            movieDir = os.path.join(tmp, "movie")
            os.makedirs(stillsDir)
            os.makedirs(movieDir)

            animator = DayAnimator(
                dayObsInt=dayObsInt,
                todaysDataDir=TEST_DATA_DIR,
                outputImageDir=stillsDir,
                outputMovieDir=movieDir,
                epoUploader=epoUploader,
                s3Uploader=s3Uploader,
                channel="all_sky_movies",
                bucketName="test-bucket",
                historical=True,
            )
            animator.run()

            stills = sorted(f for f in os.listdir(stillsDir) if f.endswith(".jpg"))
            self.assertEqual(len(stills), 2, f"expected 2 converted stills, got {stills}")
            for s in stills:
                self.assertGreater(os.path.getsize(os.path.join(stillsDir, s)), 0)

            movies = [f for f in os.listdir(movieDir) if f.endswith(".mp4")]
            self.assertEqual(len(movies), 1, f"expected exactly one movie, got {movies}")
            movieFile = os.path.join(movieDir, movies[0])
            self.assertIn("final", movies[0], "historical mode should produce a `final` movie")
            self.assertGreater(os.path.getsize(movieFile), 0)

            s3Uploader.uploadMovie.assert_called_once()
            callKwargs = s3Uploader.uploadMovie.call_args.kwargs
            self.assertEqual(callKwargs["instrument"], "allsky")
            self.assertEqual(callKwargs["dayObs"], dayObsInt)
            self.assertIsNone(callKwargs["seqNum"], "isFinal=True should pass seqNum=None")
            self.assertEqual(callKwargs["filename"], movieFile)


if __name__ == "__main__":
    unittest.main()
