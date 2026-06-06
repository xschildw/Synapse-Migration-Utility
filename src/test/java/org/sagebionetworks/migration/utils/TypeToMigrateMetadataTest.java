package org.sagebionetworks.migration.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sagebionetworks.repo.model.migration.MigrationType;
import org.sagebionetworks.repo.model.migration.MigrationTypeCount;

public class TypeToMigrateMetadataTest {

	boolean isSourceReadOnly = true;
	MigrationTypeCount src = new MigrationTypeCount().setMinid(1L).setMaxid(4L);
	MigrationTypeCount dest = new MigrationTypeCount().setMinid(5L).setMaxid(8L);

	@BeforeEach
	public void before() {
		isSourceReadOnly = true;
		src = new MigrationTypeCount().setType(MigrationType.ACTIVITY).setMinid(1L).setMaxid(4L);
		dest = new MigrationTypeCount().setType(MigrationType.ACTIVITY).setMinid(5L).setMaxid(8L);
	}

	@Test
	public void testConstructor() {
		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
	}

	@Test
	public void testConstructorWithNullSrc() {
		src = null;
		assertThrows(IllegalArgumentException.class, () -> new TypeToMigrateMetadata(isSourceReadOnly, src, dest));
	}

	@Test
	public void testConstructorWithNullDest() {
		dest = null;
		assertThrows(IllegalArgumentException.class, () -> new TypeToMigrateMetadata(isSourceReadOnly, src, dest));
	}

	@Test
	public void testConstructorWithNullSrcType() {
		src.setType(null);
		assertThrows(IllegalArgumentException.class, () -> new TypeToMigrateMetadata(isSourceReadOnly, src, dest));
	}

	@Test
	public void testConstructorWithNullDestType() {
		dest.setType(null);
		assertThrows(IllegalArgumentException.class, () -> new TypeToMigrateMetadata(isSourceReadOnly, src, dest));
	}

	@Test
	public void testConstructorWithTypesNotMatched() {
		src.setType(MigrationType.ACL);
		dest.setType(MigrationType.ACTIVITY);
		assertThrows(IllegalArgumentException.class, () -> new TypeToMigrateMetadata(isSourceReadOnly, src, dest));
	}

	@Test
	public void testGetMinOfMinsWithSrcMin() {
		src.setMinid(1L);
		dest.setMinid(3L);
		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMinOfMins();
		assertEquals(Optional.of(1L), result);
	}

	@Test
	public void testGetMinOfMinsWithDestMin() {
		src.setMinid(3L);
		dest.setMinid(1L);
		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMinOfMins();
		assertEquals(Optional.of(1L), result);
	}

	@Test
	public void testGetMinOfMinsWithDestNull() {
		src.setMinid(3L);
		dest.setMinid(null);
		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMinOfMins();
		assertEquals(Optional.of(3L), result);
	}

	@Test
	public void testGetMinOfMinsWithSrcNull() {
		src.setMinid(null);
		dest.setMinid(2L);
		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMinOfMins();
		assertEquals(Optional.of(2L), result);
	}

	@Test
	public void testGetMinOfMinsWithBothNull() {
		src.setMinid(null);
		dest.setMinid(null);
		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMinOfMins();
		assertEquals(Optional.empty(), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadOnlyAndSrcMax() {
		isSourceReadOnly = true;
		src.setMaxid(12L);
		dest.setMaxid(11L);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.of(12L), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadOnlyAndDestMax() {
		isSourceReadOnly = true;
		src.setMaxid(12L);
		dest.setMaxid(13L);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.of(13L), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadOnlyAndSrcNull() {
		isSourceReadOnly = true;
		src.setMaxid(null);
		dest.setMaxid(13L);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.of(13L), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadOnlyAndDestNull() {
		isSourceReadOnly = true;
		src.setMaxid(10L);
		dest.setMaxid(null);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.of(10L), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadOnlyAndBothNull() {
		isSourceReadOnly = true;
		src.setMaxid(null);
		dest.setMaxid(null);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.empty(), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadWriteAndDestMax() {
		isSourceReadOnly = false;
		src.setMaxid(12L);
		dest.setMaxid(13L);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.of(12L), result);
	}

	@Test
	public void testGetMaxOfMaxWithReadWriteAndNullSrc() {
		isSourceReadOnly = false;
		src.setMaxid(null);
		dest.setMaxid(13L);

		TypeToMigrateMetadata meta = new TypeToMigrateMetadata(isSourceReadOnly, src, dest);
		// call under test
		Optional<Long> result = meta.getMaxOfMax();
		assertEquals(Optional.empty(), result);
	}

}
